import functools
import logging
import operator
import os
import random
import socket
import string
import time
import uuid

from six.moves import xrange
from . import unittest

from kafka import errors, SimpleClient, KafkaConsumer, KafkaProducer
from kafka.client import KafkaClient
from kafka.cluster import ClusterMetadata
from kafka.structs import OffsetRequestPayload
from kafka.protocol.admin import CreateTopicsRequest, DeleteTopicsRequest

__all__ = [
    'random_string',
    'get_open_port',
    'kafka_versions',
    'KafkaIntegrationTestCase',
    'Timer',
]

def random_string(l):
    return "".join(random.choice(string.ascii_letters) for i in xrange(l))

def kafka_version(default=None):
    if 'KAFKA_VERSION' in os.environ:
        return os.environ.get('KAFKA_VERSION')
    else:
        return default

def kafka_versions(*versions):

    def version_str_to_list(s):
        return list(map(int, s.split('.'))) # e.g., [0, 8, 1, 1]

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
        return lambda a: op(version_str_to_list(a), version)

    validators = map(construct_lambda, versions)

    def kafka_versions(func):
        version = kafka_version()
        @functools.wraps(func)
        def wrapper(self):
            if not version:
                self.skipTest("no kafka version set in KAFKA_VERSION env var")

            for f in validators:
                if not f(version):
                    self.skipTest("unsupported kafka version")

            return func(self)
        return wrapper
    return kafka_versions

def get_open_port():
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

class KafkaIntegrationTestCase(unittest.TestCase):
    create_client = True
    topic = None
    zk = None
    server = None

    def setUp(self):
        super(KafkaIntegrationTestCase, self).setUp()
        if not kafka_version():
            self.skipTest('Integration test requires KAFKA_VERSION')

        if not self.topic:
            topic = "%s-%s" % (self.id()[self.id().rindex(".") + 1:], random_string(10))
            self.topic = topic

        if self.create_client:
            bootstrap_server = '%s:%d' % (self.server.host, self.server.port)
            self.client = SimpleClient(bootstrap_server)
            params = {
                'bootstrap_servers': bootstrap_server,
            }
            self.kafka_client = KafkaClient(**params)
            self.kafka_producer = KafkaClient(**params)
            self.kafka_consumer = KafkaConsumer(**params)

        self.create_topics([self.topic])

        self._messages = {}

    def tearDown(self):
        super(KafkaIntegrationTestCase, self).tearDown()
        if not kafka_version():
            return

        self.delete_topics([self.topic])

        if self.create_client:
            self.client.close()
            self.kafka_client.close()
            self.kafka_producer.close()
            self.kafka_consumer.close()

    def _send_request(self, request, timeout=None, success=None, failure=None):
        def _success(m):
            pass
        def _failure(e):
            raise e
        retries = 3
        while True:
            try:
                node_id = self.kafka_client.least_loaded_node()
                future = self.kafka_client.send(node_id, request)
                future.error_on_callbacks = True
                future.add_callback(_success if success is None else success)
                future.add_errback(_failure if failure is None else failure)
                return self.kafka_client.poll(future=future, timeout_ms=timeout)
            except errors.NodeNotReadyError as e:
                retries -= 1
                if retries == 0:
                    raise e
                else:
                    pass # retry

    def update_metadata(self):
        metadata = self.kafka_client.poll(future=self.kafka_client.cluster.request_update())

    def get_topics(self):
        self.update_metadata()
        return self.kafka_client.cluster.topics()

    def create_topics(self, topic_names, timeout=30000, num_partitions=1, replication_factor=1):
        topics = map(lambda t: (t, num_partitions, replication_factor, [], []), topic_names)
        request = CreateTopicsRequest[0](topics, timeout)
        result = self._send_request(request, timeout=timeout)
        for topic_result in result[0].topic_error_codes:
            error_code = topic_result[1]
            if error_code != 0:
                raise errors.for_code(error_code)
        return result

    def ensure_topics(self, topic_names, timeout=30000, num_partitions=1, replication_factor=1):
        try:
            self.create_topics(topic_names, timeout, num_partitions, replication_factor)
        except errors.TopicAlreadyExistsError:
            pass
        assert set(topic_names) == set(topic_names).intersection(set(self.get_topics()))

    def delete_topics(self, topic_names, timeout=30000):
        request = DeleteTopicsRequest[0](topic_names, timeout)
        result = self._send_request(request, timeout=timeout)
        for topic_result in result[0].topic_error_codes:
            error_code = topic_result[1]
            if error_code != 0:
                raise errors.for_code(error_code)
        return result

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

    def msgs(self, iterable):
        return [ self.msg(x) for x in iterable ]

    def msg(self, s):
        if s not in self._messages:
            self._messages[s] = '%s-%s-%s' % (s, self.id(), str(uuid.uuid4()))

        return self._messages[s].encode('utf-8')

    def key(self, k):
        return k.encode('utf-8')


class Timer(object):
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
