import abc
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

from kafka import SimpleClient
from kafka.structs import OffsetRequestPayload

__all__ = [
    'random_string',
    'get_open_port',
    'kafka_versions',
    'KafkaIntegrationSimpleApiTestCase',
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
        @functools.wraps(func)
        def wrapper(self):
            kafka_version = os.environ.get('KAFKA_VERSION')

            if not kafka_version:
                self.skipTest("no kafka version set in KAFKA_VERSION env var")

            for f in validators:
                if not f(kafka_version):
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

class KafkaIntegrationBaseTestCase(unittest.TestCase):
    __metaclass__ = abc.ABCMeta
    create_client = True
    topic = None
    zk = None
    server = None

    @abc.abstractmethod
    def get_client(cls, bootstrap_servers):
        pass

    @abc.abstractmethod
    def create_topic(cls, topic):
        pass

    @abc.abstractmethod
    def current_offset(self, topic, partition):
        pass

    def setUp(self):
        super(KafkaIntegrationBaseTestCase, self).setUp()
        if not kafka_version():
            self.skipTest('Integration test requires KAFKA_VERSION')

        if not self.topic:
            topic = "%s-%s" % (self.id()[self.id().rindex(".") + 1:], random_string(10))
            self.topic = topic

        if self.create_client:
            self.client = self.get_client()

        self.create_topic(self.topic)

        self._messages = {}

    def tearDown(self):
        super(KafkaIntegrationBaseTestCase, self).tearDown()
        if not kafka_version():
            return

        if self.create_client:
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
    
    def get_client(self, bootstrap_servers=None):
        return SimpleClient(self.bootstrap_server() if bootstrap_servers is None else bootstrap_servers)

    def create_topic(self, topic=None):
        return self.client.ensure_topic_exists(self.topic if topic is None else topic)

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


class Timer(object):
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
