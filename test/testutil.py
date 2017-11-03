import functools
import logging
import operator
import os
import random
import string
import time
import uuid
import pytest

from six.moves import xrange
from . import unittest
from test.fixtures import KafkaFixture, ZookeeperFixture, is_kerberos_enabled
from test.conftest import version, version_str_to_list

from kafka import SimpleClient
from kafka import errors
from kafka.client_async import KafkaClient
from kafka.consumer import KafkaConsumer
from kafka.producer import KafkaProducer
from kafka.structs import OffsetRequestPayload
from kafka.protocol.admin import CreateTopicsRequest, DeleteTopicsRequest
from kafka.protocol.metadata import MetadataRequest

__all__ = [
    'random_string',
    'kafka_versions',
    'KafkaIntegrationSimpleApiTestCase',
    'Timer',
]

def random_string(l):
    return "".join(random.choice(string.ascii_letters) for i in xrange(l))

def kafka_versions(*versions):

    def construct_lambda(s):
        if s[0].isdigit():
            op_str = '='
            v_str = s
        elif s[1].isdigit():
            op_str = s[0] # ! < > =
            v_str = s[1:]
        elif s[2].isdigit():
            op_str = s[0:2] # >= <=
            v_str = s[2:]
        else:
            raise ValueError('Unrecognized kafka version / operator: %s' % s)

        op_map = {
            '=': operator.eq,
            '!': operator.ne,
            '>': operator.gt,
            '<': operator.lt,
            '>=': operator.ge,
            '<=': operator.le
        }
        op = op_map[op_str]
        version = version_str_to_list(v_str)
        return lambda a: op(a, version)

    validators = map(construct_lambda, versions)

    def kafka_versions(func):
        @functools.wraps(func)
        def wrapper(self):
            kafka_version = version()

            if not kafka_version:
                self.skipTest("no kafka version set in KAFKA_VERSION env var")

            for f in validators:
                if not f(kafka_version):
                    self.skipTest("unsupported kafka version")

            return func(self)
        return wrapper
    return kafka_versions

@pytest.mark.skipif(not version(), reason='Integration test requires KAFKA_VERSION')
class KafkaIntegrationBaseTestCase(unittest.TestCase):
    zk = None
    server = None
    num_brokers = 1
    num_partitions = 1
    topic = None
    auto_create_topic = True

    @classmethod
    def setUpClass(cls):
        cls.zk = ZookeeperFixture.instance()
        cls.zk_chroot = random_string(10)
        cls.servers = []
        for i in range(cls.num_brokers):
            cls.servers.append(cls.get_broker(i, cls.num_partitions, cls.auto_create_topic))
    
        if cls.num_brokers > 0:
            cls.server = cls.servers[0] # Bootstrapping server
        
    @classmethod
    def tearDownClass(cls):
        for i in range(cls.num_brokers):
            cls.servers[i].close()
        cls.zk.close()
        pass

    @classmethod
    def get_broker(cls, inst_id, num_partitions, auto_create_topic):
        return KafkaFixture.instance(inst_id,
                                     cls.zk.host, cls.zk.port,
                                     zk_chroot=cls.zk_chroot,
                                     partitions=num_partitions,
                                     auto_create_topic=auto_create_topic,
                                     security_protocol='SASL_PLAINTEXT' if is_kerberos_enabled() else 'PLAINTEXT',
                                     sasl_mechanism='GSSAPI' if is_kerberos_enabled() else 'PLAIN')

    def setUp(self):
        super(KafkaIntegrationBaseTestCase, self).setUp()
        if not self.topic:
            self.topic = "%s-%s" % (self.id()[self.id().rindex(".") + 1:], random_string(10))

        self._messages = {}

    def tearDown(self):
        super(KafkaIntegrationBaseTestCase, self).tearDown()

        if self.client and self.create_client:
            self.client.close()

    def bootstrap_server(self):
        return '%s:%d' % (self.server.host, self.server.port)

    def msgs(self, iterable):
        return [ self.msg(x) for x in iterable ]

    def msg(self, s):
        if s not in self._messages:
            self._messages[s] = '%s-%s-%s' % (s, self.id(), str(uuid.uuid4()))

        return self._messages[s].encode('utf-8')

    def key(self, k):
        return k.encode('utf-8')


class KafkaIntegrationSimpleApiTestCase(KafkaIntegrationBaseTestCase):
    create_client = True

    def setUp(self):
        super(KafkaIntegrationSimpleApiTestCase, self).setUp()

        if self.server and self.create_client:
            self.client = SimpleClient(self.bootstrap_server())
        
        self.client.ensure_topic_exists(self.topic)

    def tearDown(self):
        if self.create_client:
            self.client.close()

        super(KafkaIntegrationSimpleApiTestCase, self).tearDown()

    def current_offset(self, topic, partition):
        try:
            offsets, = self.client.send_offset_request([OffsetRequestPayload(topic, partition, -1, 1)])
        except:
            # XXX: We've seen some UnknownErrors here and can't debug w/o server logs
            self.zk.child.dump_logs()
            self.server.child.dump_logs()
            raise
        else:
            return offsets.offsets[0]


class KafkaIntegrationStandardTestCase(KafkaIntegrationBaseTestCase):
    create_client = True
    client = None
    auto_create_topic = version() < (0,10,1,0)

    def setUp(self):
        super(KafkaIntegrationStandardTestCase, self).setUp()

        if self.server and self.create_client:
            self.client = KafkaClient(client_id='default_client',
                                      bootstrap_servers=self.bootstrap_server(),
                                      security_protocol='SASL_PLAINTEXT' if is_kerberos_enabled() else 'PLAINTEXT',
                                      sasl_mechanism='GSSAPI' if is_kerberos_enabled() else 'PLAIN')

        if self.client:
            self.ensure_topics([self.topic])

    def tearDown(self):
        #self.delete_topics([self.topic])

        if self.create_client and self.client:
            self.client.close()

        super(KafkaIntegrationStandardTestCase, self).tearDown()

    def _send_request(self, request, timeout=None, success=None, failure=None):
        def _success(m):
            pass
        def _failure(e):
            raise e
        retries = 3
        while True:
            try:
                node_id = self.client.least_loaded_node()
                future = self.client.send(node_id, request)
                future.error_on_callbacks = True
                future.add_callback(_success if success is None else success)
                future.add_errback(_failure if failure is None else failure)
                return self.client.poll(future=future, timeout_ms=timeout)
            except errors.NodeNotReadyError as e:
                retries -= 1
                if retries == 0:
                    raise e
                else:
                    pass # retry

    def create_topics(self, topic_names, timeout=30000, num_partitions=None, replication_factor=1):
        if version() >= (0, 10, 1, 0):
            if num_partitions is None:
                num_partitions = self.num_partitions
            topics = map(lambda t: (t, num_partitions, replication_factor, [], []), topic_names)
            request = CreateTopicsRequest[0](topics, timeout)
            result = self._send_request(request, timeout=timeout)
            for topic_result in result[0].topic_error_codes:
                error_code = topic_result[1]
                if error_code != 0:
                    raise errors.for_code(error_code)
            return result
        elif self.auto_create_topic:
            self._send_request(MetadataRequest[0](topic_names))
        else:
            raise RuntimeError('CreateTopicRequest is not available in Kafka versions earlier than 0.10.1.0')

    def ensure_topics(self, topic_names, timeout=30000, num_partitions=None, replication_factor=1):
        try:
            self.create_topics(topic_names, timeout, num_partitions, replication_factor)
        except errors.TopicAlreadyExistsError:
            pass
        retries = 100
        while True:
            if set(topic_names) == set(topic_names).intersection(set(self.get_topics())):
                break
            time.sleep(.1)
        assert set(topic_names) == set(topic_names).intersection(set(self.get_topics()))

    def delete_topics(self, topic_names, timeout=30000):
        request = DeleteTopicsRequest[0](topic_names, timeout)
        result = self._send_request(request, timeout=timeout)
        for topic_result in result[0].topic_error_codes:
            error_code = topic_result[1]
            if error_code != 0:
                raise errors.for_code(error_code)
        return result

    def update_metadata(self):
        return self.client.poll(future=self.client.cluster.request_update())

    def get_topics(self):
        self.update_metadata()
        return self.client.cluster.topics()

    def get_consumers(self, cnt=1, topics=None, group_id=None, heartbeat_interval_ms=500):
        if topics is None:
            topics = [self.topic]
        if not group_id:
            group_id = random_string(10)
        params = {
            'bootstrap_servers': self.bootstrap_server(),
            'group_id': group_id,
            'heartbeat_interval_ms': heartbeat_interval_ms,
        }
        return tuple(KafkaConsumer(*topics,
                                   client_id='consumer_%d' % (x,),
                                   bootstrap_servers=self.bootstrap_server(),
                                   group_id=group_id,
                                   heartbeat_interval_ms=heartbeat_interval_ms) for x in range(cnt))

    def get_producers(self, cnt=1):
        return tuple(KafkaProducer(bootstrap_servers=self.bootstrap_server()) for x in range(cnt))

#    def current_offset(self, topic, partition):
#        try:
#            offsets, = self.client.send_offset_request([OffsetRequestPayload(topic, partition, -1, 1)])
#        except:
#            # XXX: We've seen some UnknownErrors here and can't debug w/o server logs
#            self.zk.child.dump_logs()
#            self.server.child.dump_logs()
#            raise
#        else:
#            return offsets.offsets[0]


class Timer(object):
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
