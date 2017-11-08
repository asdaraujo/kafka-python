import pytest
import time

from six.moves import range

from kafka import create_message, create_gzip_message, create_snappy_message
from kafka import RoundRobinPartitioner, HashedPartitioner
from kafka.codec import has_snappy
from kafka.producer.base import Producer
from kafka.structs import FetchRequestPayload, ProduceRequestPayload
from kafka.structs import OffsetRequestPayload
from test.testutil import kafka_versions, msg, key
from test.conftest import set_params

def _assert_produce_request(client, topic, messages, initial_offset, message_ct, partition=0):
    produce = ProduceRequestPayload(topic, partition, messages=messages)

    # There should only be one response message from the server.
    # This will throw an exception if there's more than one.
    resp = client.send_produce_request([ produce ])
    _assert_produce_response(resp, initial_offset)

    assert _current_offset(client, topic, partition) == initial_offset + message_ct

def _assert_produce_response(resp, initial_offset):
    assert len(resp) == 1
    assert resp[0].error == 0
    assert resp[0].offset == initial_offset

def _assert_fetch_offset(client, topic, partition, start_offset, expected_messages):
    # There should only be one response message from the server.
    # This will throw an exception if there's more than one.

    resp, = client.send_fetch_request([FetchRequestPayload(topic, partition, start_offset, 1024)])

    assert resp.error == 0
    assert resp.partition == partition
    messages = [ x.message.value for x in resp.messages ]

    assert messages == expected_messages
    assert resp.highwaterMark == start_offset+len(expected_messages)

def _current_offset(client, topic, partition):
    offsets, = client.send_offset_request([OffsetRequestPayload(topic, partition, -1, 1)])
    return offsets.offsets[0]

def test_produce_many_simple(simple_client, topic):
    start_offset = _current_offset(simple_client, topic, 0)

    _assert_produce_request(
        simple_client, topic,
        [create_message(("Test message %d" % i).encode('utf-8'))
         for i in range(100)],
        start_offset,
        100,
    )

    _assert_produce_request(
        simple_client, topic,
        [create_message(("Test message %d" % i).encode('utf-8'))
         for i in range(100)],
        start_offset+100,
        100,
    )

def test_produce_10k_simple(simple_client, topic):
    start_offset = _current_offset(simple_client, topic, 0)

    _assert_produce_request(simple_client, topic, 
        [create_message(("Test message %d" % i).encode('utf-8'))
         for i in range(10000)],
        start_offset,
        10000,
    )

def test_produce_many_gzip(simple_client, topic):
    start_offset = _current_offset(simple_client, topic, 0)

    message1 = create_gzip_message([
        (("Gzipped 1 %d" % i).encode('utf-8'), None) for i in range(100)])
    message2 = create_gzip_message([
        (("Gzipped 2 %d" % i).encode('utf-8'), None) for i in range(100)])

    _assert_produce_request(simple_client, topic, 
        [ message1, message2 ],
        start_offset,
        200,
    )

def test_produce_many_snappy(simple_client, topic):
    pytest.skip("All snappy integration tests fail with nosnappyjava")
    start_offset = _current_offset(simple_client, topic, 0)

    _assert_produce_request(simple_client, topic, [
            create_snappy_message([("Snappy 1 %d" % i, None) for i in range(100)]),
            create_snappy_message([("Snappy 2 %d" % i, None) for i in range(100)]),
        ],
        start_offset,
        200,
    )

def test_produce_mixed(simple_client, topic):
    start_offset = _current_offset(simple_client, topic, 0)

    msg_count = 1+100
    messages = [
        create_message(b"Just a plain message"),
        create_gzip_message([
            (("Gzipped %d" % i).encode('utf-8'), None) for i in range(100)]),
    ]

    # All snappy integration tests fail with nosnappyjava
    if False and has_snappy():
        msg_count += 100
        messages.append(create_snappy_message([("Snappy %d" % i, None) for i in range(100)]))

    _assert_produce_request(simple_client, topic, messages, start_offset, msg_count)

def test_produce_100k_gzipped(simple_client, topic):
    start_offset = _current_offset(simple_client, topic, 0)

    _assert_produce_request(simple_client, topic, [
        create_gzip_message([
            (("Gzipped batch 1, message %d" % i).encode('utf-8'), None)
            for i in range(50000)])
        ],
        start_offset,
        50000,
    )

    _assert_produce_request(simple_client, topic, [
        create_gzip_message([
            (("Gzipped batch 1, message %d" % i).encode('utf-8'), None)
            for i in range(50000)])
        ],
        start_offset+50000,
        50000,
    )

############################
#   SimpleProducer Tests   #
############################

def test_simple_producer_new_topic(simple_client, topic, simple_producer):
    resp = simple_producer.send_messages('new_topic', msg('foobar'))
    _assert_produce_response(resp, 0)

@set_params(simple_producer={'random_start':False})
def test_simple_producer(simple_client, topic, simple_producer):
    partitions = simple_client.get_partition_ids_for_topic(topic)
    start_offsets = [_current_offset(simple_client, topic, p) for p in partitions]

    # Goes to first partition, randomly.
    resp = simple_producer.send_messages(topic, msg("one"), msg("two"))
    _assert_produce_response(resp, start_offsets[0])

    # Goes to the next partition, randomly.
    resp = simple_producer.send_messages(topic, msg("three"))
    _assert_produce_response(resp, start_offsets[1])

    _assert_fetch_offset(simple_client, topic, partitions[0], start_offsets[0], [ msg("one"), msg("two") ])
    _assert_fetch_offset(simple_client, topic, partitions[1], start_offsets[1], [ msg("three") ])

    # Goes back to the first partition because there's only two partitions
    resp = simple_producer.send_messages(topic, msg("four"), msg("five"))
    _assert_produce_response(resp, start_offsets[0]+2)
    _assert_fetch_offset(simple_client, topic, partitions[0], start_offsets[0], [ msg("one"), msg("two"), msg("four"), msg("five") ])

@set_params(simple_producer={'random_start':False})
def test_producer_random_order(simple_client, topic, simple_producer):
    resp1 = simple_producer.send_messages(topic, msg("one"), msg("two"))
    resp2 = simple_producer.send_messages(topic, msg("three"))
    resp3 = simple_producer.send_messages(topic, msg("four"), msg("five"))

    assert resp1[0].partition == resp3[0].partition
    assert resp1[0].partition != resp2[0].partition

@set_params(simple_producer={'random_start':False})
def test_producer_ordered_start(simple_client, topic, simple_producer):
    resp1 = simple_producer.send_messages(topic, msg("one"), msg("two"))
    resp2 = simple_producer.send_messages(topic, msg("three"))
    resp3 = simple_producer.send_messages(topic, msg("four"), msg("five"))

    assert resp1[0].partition == 0
    assert resp2[0].partition == 1
    assert resp3[0].partition == 0

@set_params(simple_producer={'random_start':False, 'async':True})
def test_async_simple_producer(simple_client, topic, simple_producer):
    partition = simple_client.get_partition_ids_for_topic(topic)[0]
    start_offset = _current_offset(simple_client, topic, partition)

    resp = simple_producer.send_messages(topic, msg("one"))
    assert len(resp) == 0

    # flush messages
    simple_producer.stop()

    _assert_fetch_offset(simple_client, topic, partition, start_offset, [ msg("one") ])

@set_params(simple_producer={'random_start':False, 'async':True, 'batch_send_every_n':5, 'batch_send_every_t':5})
def test_batched_simple_producer__triggers_by_message(simple_client, topic, simple_producer):
    partitions = simple_client.get_partition_ids_for_topic(topic)
    start_offsets = [_current_offset(simple_client, topic, p) for p in partitions]

    # Send 4 messages -- should not trigger a batch
    resp = simple_producer.send_messages(
        topic,
        msg("one"),
        msg("two"),
        msg("three"),
        msg("four"),
    )

    # Batch mode is async. No ack
    assert len(resp) == 0

    # It hasn't sent yet
    _assert_fetch_offset(simple_client, topic, partitions[0], start_offsets[0], [])
    _assert_fetch_offset(simple_client, topic, partitions[1], start_offsets[1], [])

    # send 3 more messages -- should trigger batch on first 5
    resp = simple_producer.send_messages(
        topic,
        msg("five"),
        msg("six"),
        msg("seven"),
    )

    # Batch mode is async. No ack
    assert len(resp) == 0

    # Wait until producer has pulled all messages from internal queue
    # this should signal that the first batch was sent, and the producer
    # is now waiting for enough messages to batch again (or a timeout)
    timeout = 5
    start = time.time()
    while not simple_producer.queue.empty():
        if time.time() - start > timeout:
            pytest.fail('timeout waiting for producer queue to empty')
        time.sleep(0.1)

    # send messages groups all *msgs in a single call to the same partition
    # so we should see all messages from the first call in one partition
    _assert_fetch_offset(simple_client, topic, partitions[0], start_offsets[0], [
        msg("one"),
        msg("two"),
        msg("three"),
        msg("four"),
    ])

    # Because we are batching every 5 messages, we should only see one
    _assert_fetch_offset(simple_client, topic, partitions[1], start_offsets[1], [
        msg("five"),
    ])

@set_params(simple_producer={'random_start':False, 'async':True, 'batch_send_every_n':100, 'batch_send_every_t':5})
def test_batched_simple_producer__triggers_by_time(simple_client, topic, simple_producer):
    partitions = simple_client.get_partition_ids_for_topic(topic)
    start_offsets = [_current_offset(simple_client, topic, p) for p in partitions]

    # Send 5 messages and do a fetch
    resp = simple_producer.send_messages(
        topic,
        msg("one"),
        msg("two"),
        msg("three"),
        msg("four"),
    )

    # Batch mode is async. No ack
    assert len(resp) == 0

    # It hasn't sent yet
    _assert_fetch_offset(simple_client, topic, partitions[0], start_offsets[0], [])
    _assert_fetch_offset(simple_client, topic, partitions[1], start_offsets[1], [])

    resp = simple_producer.send_messages(topic,
        msg("five"),
        msg("six"),
        msg("seven"),
    )

    # Batch mode is async. No ack
    assert len(resp) == 0

    # Wait the timeout out - batch_interval
    time.sleep(5)

    _assert_fetch_offset(simple_client, topic, partitions[0], start_offsets[0], [
        msg("one"),
        msg("two"),
        msg("three"),
        msg("four"),
    ])

    _assert_fetch_offset(simple_client, topic, partitions[1], start_offsets[1], [
        msg("five"),
        msg("six"),
        msg("seven"),
    ])

############################
#   KeyedProducer Tests    #
############################

@kafka_versions('>=0.8.1')
@set_params(keyed_producer={'partitioner':RoundRobinPartitioner})
def test_keyedproducer_null_payload(simple_client, topic, keyed_producer):
    partitions = simple_client.get_partition_ids_for_topic(topic)
    start_offsets = [_current_offset(simple_client, topic, p) for p in partitions]

    resp = keyed_producer.send_messages(topic, key("key1"), msg("one"))
    _assert_produce_response(resp, start_offsets[0])
    resp = keyed_producer.send_messages(topic, key("key2"), None)
    _assert_produce_response(resp, start_offsets[1])
    resp = keyed_producer.send_messages(topic, key("key3"), None)
    _assert_produce_response(resp, start_offsets[0]+1)
    resp = keyed_producer.send_messages(topic, key("key4"), msg("four"))
    _assert_produce_response(resp, start_offsets[1]+1)

    _assert_fetch_offset(simple_client, topic, partitions[0], start_offsets[0], [ msg("one"), None ])
    _assert_fetch_offset(simple_client, topic, partitions[1], start_offsets[1], [ None, msg("four") ])

@set_params(keyed_producer={'partitioner':RoundRobinPartitioner})
def test_round_robin_partitioner(simple_client, topic, keyed_producer):
    partitions = simple_client.get_partition_ids_for_topic(topic)
    start_offsets = [_current_offset(simple_client, topic, p) for p in partitions]

    resp1 = keyed_producer.send_messages(topic, key("key1"), msg("one"))
    resp2 = keyed_producer.send_messages(topic, key("key2"), msg("two"))
    resp3 = keyed_producer.send_messages(topic, key("key3"), msg("three"))
    resp4 = keyed_producer.send_messages(topic, key("key4"), msg("four"))

    _assert_produce_response(resp1, start_offsets[0]+0)
    _assert_produce_response(resp2, start_offsets[1]+0)
    _assert_produce_response(resp3, start_offsets[0]+1)
    _assert_produce_response(resp4, start_offsets[1]+1)

    _assert_fetch_offset(simple_client, topic, partitions[0], start_offsets[0], [ msg("one"), msg("three") ])
    _assert_fetch_offset(simple_client, topic, partitions[1], start_offsets[1], [ msg("two"), msg("four")  ])

@set_params(keyed_producer={'partitioner':HashedPartitioner})
def test_hashed_partitioner(simple_client, topic, keyed_producer):
    partitions = simple_client.get_partition_ids_for_topic(topic)
    start_offsets = [_current_offset(simple_client, topic, p) for p in partitions]

    resp1 = keyed_producer.send_messages(topic, key("1"), msg("one"))
    resp2 = keyed_producer.send_messages(topic, key("2"), msg("two"))
    resp3 = keyed_producer.send_messages(topic, key("3"), msg("three"))
    resp4 = keyed_producer.send_messages(topic, key("3"), msg("four"))
    resp5 = keyed_producer.send_messages(topic, key("4"), msg("five"))

    offsets = {partitions[0]: start_offsets[0], partitions[1]: start_offsets[1]}
    messages = {partitions[0]: [], partitions[1]: []}

    keys = [key(k) for k in ["1", "2", "3", "3", "4"]]
    resps = [resp1, resp2, resp3, resp4, resp5]
    msgs = [msg(m) for m in ["one", "two", "three", "four", "five"]]

    for a_key, resp, a_msg in zip(keys, resps, msgs):
        k = hash(a_key) % 2
        partition = partitions[k]
        offset = offsets[partition]
        _assert_produce_response(resp, offset)
        offsets[partition] += 1
        messages[partition].append(a_msg)

    _assert_fetch_offset(simple_client, topic, partitions[0], start_offsets[0], messages[partitions[0]])
    _assert_fetch_offset(simple_client, topic, partitions[1], start_offsets[1], messages[partitions[1]])

@set_params(keyed_producer={'partitioner':RoundRobinPartitioner, 'async':True, 'batch_send_every_t':1})
def test_async_keyed_producer(simple_client, topic, keyed_producer):
    partition = simple_client.get_partition_ids_for_topic(topic)[0]
    start_offset = _current_offset(simple_client, topic, partition)

    resp = keyed_producer.send_messages(topic, key("key1"), msg("one"))
    assert len(resp) == 0

    # wait for the server to report a new highwatermark
    while _current_offset(simple_client, topic, partition) == start_offset:
        time.sleep(0.1)

    _assert_fetch_offset(simple_client, topic, partition, start_offset, [ msg("one") ])

############################
#   Producer ACK Tests     #
############################

@set_params(base_producer={'req_acks':Producer.ACK_NOT_REQUIRED})
def test_acks_none(simple_client, topic, base_producer):
    partition = simple_client.get_partition_ids_for_topic(topic)[0]
    start_offset = _current_offset(simple_client, topic, partition)

    resp = base_producer.send_messages(topic, partition, msg("one"))

    # No response from produce request with no acks required
    assert len(resp) == 0

    # But the message should still have been delivered
    _assert_fetch_offset(simple_client, topic, partition, start_offset, [ msg("one") ])

@set_params(base_producer={'req_acks':Producer.ACK_AFTER_LOCAL_WRITE})
def test_acks_local_write(simple_client, topic, base_producer):
    partition = simple_client.get_partition_ids_for_topic(topic)[0]
    start_offset = _current_offset(simple_client, topic, partition)

    resp = base_producer.send_messages(topic, partition, msg("one"))

    _assert_produce_response(resp, start_offset)
    _assert_fetch_offset(simple_client, topic, partition, start_offset, [ msg("one") ])

@set_params(base_producer={'req_acks':Producer.ACK_AFTER_CLUSTER_COMMIT})
def test_acks_cluster_commit(simple_client, topic, base_producer):
    partition = simple_client.get_partition_ids_for_topic(topic)[0]
    start_offset = _current_offset(simple_client, topic, partition)

    resp = base_producer.send_messages(topic, partition, msg("one"))
    _assert_produce_response(resp, start_offset)
    _assert_fetch_offset(simple_client, topic, partition, start_offset, [ msg("one") ])


