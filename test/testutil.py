import decorator
import operator
import os
import pytest
import random
import string
import time
import uuid

from six.moves import xrange

from kafka import create_message, create_gzip_message, errors
from kafka.structs import ProduceRequestPayload
from test.fixtures import get_simple_consumer, random_string

__all__ = [
    'kafka_versions',
]

def kafka_version():
    if 'KAFKA_VERSION' not in os.environ:
        return ()
    return version_str_to_list(os.environ['KAFKA_VERSION'])

def version_str_to_list(s):
    return tuple(map(int, s.split('.'))) # e.g., [0, 8, 1, 1]

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
        def wrapper(func, *args, **kwargs):
            version = kafka_version()

            if not version:
                pytest.skip("no kafka version set in KAFKA_VERSION env var")

            for f in validators:
                if not f(version):
                    pytest.skip("unsupported kafka version")

            return func(*args, **kwargs)
        return decorator.decorator(wrapper, func)

    return kafka_versions

_messages = {}
def msg(s):
    global _messages
    if s not in _messages:
        _messages[s] = '%s-%s' % (s, str(uuid.uuid4()))

    return _messages[s].encode('utf-8')

def key(k):
    return k.encode('utf-8')

def send_messages(client, topic, partition, messages):
    messages = [ create_message(msg(str(m))) for m in messages ]
    produce = ProduceRequestPayload(topic, partition, messages = messages)
    resp, = client.send_produce_request([produce])
    assert resp.error == 0

    return [ x.value for x in messages ]

def send_gzip_message(client, topic, partition, messages):
    message = create_gzip_message([(msg(str(m)), None) for m in messages])
    produce = ProduceRequestPayload(topic, partition, messages = [message])
    resp, = client.send_produce_request([produce])
    assert resp.error == 0

def assert_message_count(messages, num_messages):
    # Make sure we got them all
    assert len(messages) == num_messages

    # Make sure there are no duplicates
    assert len(set(messages)) == num_messages


def assert_topic_message_count(simple_client, topic, check_count, timeout=10,
                               partitions=None, at_least=False):
    simple_consumer = get_simple_consumer(simple_client, topic,
                                          partitions=partitions,
                                          auto_commit=False,
                                          iter_timeout=timeout)
    started_at = time.time()
    pending = -1
    while pending < check_count and (time.time() - started_at < timeout):
        try:
            pending = simple_consumer.pending(partitions)
        except errors.FailedPayloadsError:
            pass
        time.sleep(0.5)
    simple_consumer.stop()

    if pending < check_count:
        pytest.fail('Too few pending messages: found %d, expected %d' %
                  (pending, check_count))
    elif pending > check_count and not at_least:
        pytest.fail('Too many pending messages: found %d, expected %d' %
                  (pending, check_count))

def wait_for_kafka_client_topic_update(client, expected_topics):
    for t in range(1000):
        client.poll(future=client.cluster.request_update())
        client_topics = client.cluster.topics()
        if set(expected_topics) == set(client_topics):
            break
        time.sleep(.01)
    else:
        assert set(expected_topics).intersection(client_topics) == set(expected_topics)

def wait_for_simple_client_topic_update(client, expected_topics):
    for t in range(1000):
        client_topics = set(map(lambda tp: tp.topic, client.topics_to_brokers.keys()))
        if set(expected_topics) == set(client_topics):
            break
        client.load_metadata_for_topics()
        time.sleep(.01)

class Timer(object):
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
