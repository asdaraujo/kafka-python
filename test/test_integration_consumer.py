import logging
import pytest
import six
import time

from six.moves import xrange

from kafka.errors import UnsupportedVersionError, KafkaTimeoutError
from kafka.structs import TopicPartition, OffsetAndTimestamp
from test.conftest import set_params
from test.testutil import kafka_versions, send_messages, assert_message_count, Timer

@set_params(kafka_consumer={'auto_offset_reset':'earliest'})
def test_kafka_consumer(simple_client, topic, kafka_consumer):
    send_messages(simple_client, topic, 0, range(0, 100))
    send_messages(simple_client, topic, 1, range(100, 200))

    n = 0
    messages = {0: set(), 1: set()}
    for m in kafka_consumer:
        logging.debug("Consumed message %s" % repr(m))
        n += 1
        messages[m.partition].add(m.offset)
        if n >= 200:
            break

    assert len(messages[0]) == 100
    assert len(messages[1]) == 100

TIMEOUT_MS=500
@set_params(kafka_consumer={'auto_offset_reset':'earliest','enable_auto_commit':False,'consumer_timeout_ms':TIMEOUT_MS})
def test_kafka_consumer__blocking(simple_client, topic, kafka_consumer):
    # Manual assignment avoids overhead of consumer group mgmt
    kafka_consumer.unsubscribe()
    kafka_consumer.assign([TopicPartition(topic, 0)])

    # Ask for 5 messages, nothing in queue, block 500ms
    with Timer() as t:
        with pytest.raises(StopIteration):
            msg = next(kafka_consumer)
    assert t.interval >= TIMEOUT_MS / 1000.0

    send_messages(simple_client, topic, 0, range(0, 10))

    # Ask for 5 messages, 10 in queue. Get 5 back, no blocking
    messages = set()
    with Timer() as t:
        for i in range(5):
            msg = next(kafka_consumer)
            messages.add((msg.partition, msg.offset))
    assert len(messages) == 5
    assert t.interval < TIMEOUT_MS / 1000.0

    # Ask for 10 messages, get 5 back, block 500ms
    messages = set()
    with Timer() as t:
        with pytest.raises(StopIteration):
            for i in range(10):
                msg = next(kafka_consumer)
                messages.add((msg.partition, msg.offset))
    assert len(messages) == 5
    assert t.interval >= TIMEOUT_MS / 1000.0

@kafka_versions('>=0.8.1')
def test_kafka_consumer__offset_commit_resume(simple_client, topic, kafka_broker):
    send_messages(simple_client, topic, 0, range(0, 100))
    send_messages(simple_client, topic, 1, range(100, 200))

    # Start a consumer
    group_id = 'test_kafka_consumer__offset_commit_resume'
    (consumer1,) = kafka_broker.get_consumers(1, [topic],
                                              group_id=group_id,
                                              enable_auto_commit=True,
                                              auto_commit_interval_ms=100,
                                              auto_offset_reset='earliest')

    # Grab the first 180 messages
    output_msgs1 = []
    for _ in xrange(180):
        m = next(consumer1)
        output_msgs1.append(m)
    assert_message_count(output_msgs1, 180)
    consumer1.close()

    # The total offset across both partitions should be at 180
    (consumer2,) = kafka_broker.get_consumers(1, [topic],
                                              group_id=group_id,
                                              enable_auto_commit=True,
                                              auto_commit_interval_ms=100,
                                              auto_offset_reset='earliest')

    # 181-200
    output_msgs2 = []
    for _ in xrange(20):
        m = next(consumer2)
        output_msgs2.append(m)
    assert_message_count(output_msgs2, 20)
    assert len(set(output_msgs1) | set(output_msgs2)) == 200

@kafka_versions('>=0.10.1')
@set_params(kafka_consumer={'auto_offset_reset':'earliest', 'fetch_max_bytes':825})
def test_kafka_consumer_max_bytes_simple(simple_client, topic, kafka_consumer):
    send_messages(simple_client, topic, 0, range(100, 200))
    send_messages(simple_client, topic, 1, range(200, 300))

    seen_partitions = set([])
    for i in range(10):
        poll_res = kafka_consumer.poll(timeout_ms=100)
        for partition, msgs in six.iteritems(poll_res):
            for msg in msgs:
                seen_partitions.add(partition)

    # Check that we fetched at least 1 message from both partitions
    assert seen_partitions == set([
            TopicPartition(topic, 0), TopicPartition(topic, 1)])

@kafka_versions('>=0.10.1')
@set_params(kafka_consumer={'group_id':'test-kafka-consumer-max-bytes-one-msg', 'auto_offset_reset':'earliest', 'consumer_timeout_ms':5000, 'fetch_max_bytes':1})
def test_kafka_consumer_max_bytes_one_msg(simple_client, topic, kafka_consumer):
    # We send to only 1 partition so we don't have parallel requests to 2
    # nodes for data.
    send_messages(simple_client, topic, 0, range(100, 200))

    # Start a consumer. FetchResponse_v3 should always include at least 1
    # full msg, so by setting fetch_max_bytes=1 we should get 1 msg at a time
    # But 0.11.0.0 returns 1 MessageSet at a time when the messages are
    # stored in the new v2 format by the broker.
    #
    # DP Note: This is a strange test. The consumer shouldn't care
    # how many messages are included in a FetchResponse, as long as it is
    # non-zero. I would not mind if we deleted this test. It caused
    # a minor headache when testing 0.11.0.0.
    fetched_msgs = [next(kafka_consumer) for i in range(10)]
    assert len(fetched_msgs) == 10

@kafka_versions('>=0.10.1')
def test_kafka_consumer_offsets_for_time(simple_client, topic, kafka_consumer, kafka_producer):
    late_time = int(time.time()) * 1000
    middle_time = late_time - 1000
    early_time = late_time - 2000
    tp = TopicPartition(topic, 0)

    early_msg = kafka_producer.send(
        topic, partition=0, value=b"first",
        timestamp_ms=early_time).get(1)
    late_msg = kafka_producer.send(
        topic, partition=0, value=b"last",
        timestamp_ms=late_time).get(1)

    offsets = kafka_consumer.offsets_for_times({tp: early_time})
    assert len(offsets) == 1
    assert offsets[tp].offset == early_msg.offset
    assert offsets[tp].timestamp == early_time

    offsets = kafka_consumer.offsets_for_times({tp: middle_time})
    assert offsets[tp].offset == late_msg.offset
    assert offsets[tp].timestamp == late_time

    offsets = kafka_consumer.offsets_for_times({tp: late_time})
    assert offsets[tp].offset == late_msg.offset
    assert offsets[tp].timestamp == late_time

    offsets = kafka_consumer.offsets_for_times({})
    assert offsets == {}

    # Out of bound timestamps check

    offsets = kafka_consumer.offsets_for_times({tp: 0})
    assert offsets[tp].offset == early_msg.offset
    assert offsets[tp].timestamp == early_time

    offsets = kafka_consumer.offsets_for_times({tp: 9999999999999})
    assert offsets[tp] is None

    # Beginning/End offsets

    offsets = kafka_consumer.beginning_offsets([tp])
    assert offsets == { tp: early_msg.offset }
    offsets = kafka_consumer.end_offsets([tp])
    assert offsets == { tp: late_msg.offset + 1 }

@kafka_versions('>=0.10.1')
def test_kafka_consumer_offsets_search_many_partitions(simple_client, topic, kafka_consumer, kafka_producer):
    tp0 = TopicPartition(topic, 0)
    tp1 = TopicPartition(topic, 1)

    send_time = int(time.time() * 1000)
    p0msg = kafka_producer.send(
        topic, partition=0, value=b"XXX",
        timestamp_ms=send_time).get()
    p1msg = kafka_producer.send(
        topic, partition=1, value=b"XXX",
        timestamp_ms=send_time).get()

    offsets = kafka_consumer.offsets_for_times({
        tp0: send_time,
        tp1: send_time
    })

    assert offsets == {
        tp0: OffsetAndTimestamp(p0msg.offset, send_time),
        tp1: OffsetAndTimestamp(p1msg.offset, send_time)
    }

    offsets = kafka_consumer.beginning_offsets([tp0, tp1])
    assert offsets == {
        tp0: p0msg.offset,
        tp1: p1msg.offset
    }

    offsets = kafka_consumer.end_offsets([tp0, tp1])
    assert offsets == {
        tp0: p0msg.offset + 1,
        tp1: p1msg.offset + 1
    }

@kafka_versions('<0.10.1')
def test_kafka_consumer_offsets_for_time_old(simple_client, topic, kafka_consumer):
    tp = TopicPartition(topic, 0)

    with pytest.raises(UnsupportedVersionError):
        kafka_consumer.offsets_for_times({tp: int(time.time())})

@kafka_versions('>=0.10.1')
def test_kafka_consumer_offsets_for_times_errors(simple_client, topic, kafka_consumer):
    tp = TopicPartition(topic, 0)
    bad_tp = TopicPartition(topic, 100)

    with pytest.raises(ValueError):
        kafka_consumer.offsets_for_times({tp: -1})

    with pytest.raises(KafkaTimeoutError):
        kafka_consumer.offsets_for_times({bad_tp: 0})
