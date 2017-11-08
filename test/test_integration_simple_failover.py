import atexit
import logging
import pytest
import time

from kafka.errors import (
    FailedPayloadsError, ConnectionError, RequestTimedOutError,
    NotLeaderForPartitionError)
from kafka.producer.base import Producer
from kafka.protocol.metadata import MetadataRequest
from kafka.structs import TopicPartition
from test.testutil import random_string, assert_topic_message_count
from test.conftest import set_params, set_broker_params

log = logging.getLogger(__name__)

set_broker_params(num_brokers=3,
                  zk_chroot='broker_replicas_%s' % (random_string(6),),
                  replicas=3,
                  partitions=3)

def _send_random_messages(producer, topic, partition, n):
    for j in range(n):
        msg = 'msg {0}: {1}'.format(j, random_string(10))
        log.debug('_send_random_message %s to %s:%d', msg, topic, partition)
        while True:
            try:
                producer.send_messages(topic, partition, msg.encode('utf-8'))
            except:
                log.exception('failure in _send_random_messages - retrying')
                continue
            else:
                break

def _stop_leader(brokers, client, topic, partition):
    leader = client.topics_to_brokers[TopicPartition(topic, partition)]
    broker = brokers[leader.nodeId]
    broker.stop()
    return broker

def _start_leader(broker):
    broker.open()
    
@set_params(base_producer={'async':False,
                           'req_acks':Producer.ACK_AFTER_CLUSTER_COMMIT})
def test_switch_leader(kafka_brokers, simple_client, topic, base_producer):
    partition = 0

    # Testing the base Producer class here so that we can easily send
    # messages to a specific partition, kill the leader for that partition
    # and check that after another broker takes leadership the producer
    # is able to resume sending messages

    # require that the server commit messages to all in-sync replicas
    # so that failover doesn't lose any messages on server-side
    # and we can assert that server-side message count equals client-side

    # Send 100 random messages to a specific partition
    _send_random_messages(base_producer, topic, partition, 100)

    # kill leader for partition
    leader = _stop_leader(kafka_brokers, simple_client, topic, partition)

    # expect failure, but don't wait more than 60 secs to recover
    recovered = False
    started = time.time()
    timeout = 60
    while (time.time() - started) < timeout:
        try:
            log.debug("attempting to send 'success' message after leader killed")
            base_producer.send_messages(topic, partition, b'success')
            log.debug("success!")
            recovered = True
            break
        except (FailedPayloadsError, ConnectionError, RequestTimedOutError,
                NotLeaderForPartitionError) as e:
            log.debug("caught exception sending message -- will retry")
        time.sleep(.5)

    # Verify we successfully sent the message
    assert recovered

    # send some more messages to new leader
    _send_random_messages(base_producer, topic, partition, 100)

    # count number of messages
    # Should be equal to 100 before + 1 recovery + 100 after
    # at_least=True because exactly once delivery isn't really a thing
    assert_topic_message_count(simple_client, topic, 201, partitions=(partition,), at_least=True)

    _start_leader(leader)

@set_params(base_producer={'async':True,
                           'batch_send_every_n':15,
                           'batch_send_every_t':3,
                           'req_acks':Producer.ACK_AFTER_CLUSTER_COMMIT,
                           'async_log_messages_on_error':False})
def test_switch_leader_async(kafka_brokers, simple_client, topic, base_producer):
    partition = 0

    # Test the base class Producer -- send_messages to a specific partition
    # Send 10 random messages
    _send_random_messages(base_producer, topic, partition, 10)
    _send_random_messages(base_producer, topic, partition + 1, 10)

    # kill leader for partition
    leader = _stop_leader(kafka_brokers, simple_client, topic, partition)

    log.debug("attempting to send 'success' message after leader killed")

    # in async mode, this should return immediately
    base_producer.send_messages(topic, partition, b'success')
    base_producer.send_messages(topic, partition + 1, b'success')

    # send to new leader
    _send_random_messages(base_producer, topic, partition, 10)
    _send_random_messages(base_producer, topic, partition + 1, 10)

    # Stop the producer and wait for it to shutdown
    base_producer.stop()
    started = time.time()
    timeout = 60
    while (time.time() - started) < timeout:
        if not base_producer.thread.is_alive():
            break
        time.sleep(0.1)
    else:
        pytest.fail('timeout waiting for producer queue to empty')

    # count number of messages
    # Should be equal to 10 before + 1 recovery + 10 after
    # at_least=True because exactly once delivery isn't really a thing
    assert_topic_message_count(simple_client, topic, 21, partitions=(partition,), at_least=True)
    assert_topic_message_count(simple_client, topic, 21, partitions=(partition + 1,), at_least=True)

    _start_leader(leader)

@set_params(keyed_producer={'async':False})
def test_switch_leader_keyed_producer(kafka_brokers, simple_client, topic, keyed_producer):
    # Send 10 random messages
    for _ in range(10):
        key = random_string(3).encode('utf-8')
        msg = random_string(10).encode('utf-8')
        keyed_producer.send_messages(topic, key, msg)

    # kill leader for partition 0
    leader = _stop_leader(kafka_brokers, simple_client, topic, 0)

    recovered = False
    started = time.time()
    timeout = 60
    while not recovered and (time.time() - started) < timeout:
        try:
            key = random_string(3).encode('utf-8')
            msg = random_string(10).encode('utf-8')
            keyed_producer.send_messages(topic, key, msg)
            if keyed_producer.partitioners[topic].partition(key) == 0:
                recovered = True
        except (FailedPayloadsError, ConnectionError, RequestTimedOutError,
                NotLeaderForPartitionError):
            log.debug("caught exception sending message -- will retry")
            continue

    # Verify we successfully sent the message
    assert recovered

    # send some more messages just to make sure no more exceptions
    for _ in range(10):
        key = random_string(3).encode('utf-8')
        msg = random_string(10).encode('utf-8')
        keyed_producer.send_messages(topic, key, msg)

    _start_leader(leader)

@set_params(base_producer={'async':False},
            simple_consumer={'auto_commit':False,
                             'iter_timeout':10})
def test_switch_leader_simple_consumer(kafka_brokers, simple_client, topic, base_producer, simple_consumer):
    _send_random_messages(base_producer, topic, 0, 2)
    simple_consumer.get_messages()
    leader = _stop_leader(kafka_brokers, simple_client, topic, 0)
    simple_consumer.get_messages()
    _start_leader(leader)
