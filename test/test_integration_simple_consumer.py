import os
import pytest

from six.moves import xrange

from kafka import MultiProcessConsumer, SimpleConsumer, create_message, create_gzip_message
from kafka.consumer.base import MAX_FETCH_BUFFER_SIZE_BYTES
from kafka.errors import ConsumerFetchSizeTooSmall, OffsetOutOfRangeError
from kafka.structs import ProduceRequestPayload
from test.conftest import set_params
from test.fixtures import get_simple_consumer
from test.testutil import kafka_versions, random_string, msg, Timer, send_messages, send_gzip_message, assert_message_count

def _consumer(client, topic, **kwargs):
    if os.environ['KAFKA_VERSION'] == "0.8.0":
        # Kafka 0.8.0 simply doesn't support offset requests, so hard code it being off
        kwargs['group'] = None
        kwargs['auto_commit'] = False
    else:
        kwargs.setdefault('group', None)
        kwargs.setdefault('auto_commit', False)

    consumer_class = kwargs.pop('consumer', SimpleConsumer)
    group = kwargs.pop('group', None)
    topic = kwargs.pop('topic', topic)

    if consumer_class in [SimpleConsumer, MultiProcessConsumer]:
        kwargs.setdefault('iter_timeout', 0)

    return consumer_class(client, group, topic, **kwargs)

def test_simple_consumer(simple_client, simple_consumer, topic):
    send_messages(simple_client, topic, 0, range(0, 100))
    send_messages(simple_client, topic, 1, range(100, 200))

    assert_message_count([ message for message in simple_consumer ], 200)

def test_simple_consumer_gzip(simple_client, simple_consumer, topic):
    send_gzip_message(simple_client, topic, 0, range(0, 100))
    send_gzip_message(simple_client, topic, 1, range(100, 200))

    assert_message_count([ message for message in simple_consumer ], 200)

@set_params(simple_consumer={'auto_offset_reset':'smallest'})
def test_simple_consumer_smallest_offset_reset(simple_client, simple_consumer, topic):
    send_messages(simple_client, topic, 0, range(0, 100))
    send_messages(simple_client, topic, 1, range(100, 200))

    # Move fetch offset ahead of 300 message (out of range)
    simple_consumer.seek(300, 2)
    # Since auto_offset_reset is set to smallest we should read all 200
    # messages from beginning.
    assert_message_count([message for message in simple_consumer], 200)

def test_simple_consumer_largest_offset_reset(simple_client, simple_consumer, topic):
    send_messages(simple_client, topic, 0, range(0, 100))
    send_messages(simple_client, topic, 1, range(100, 200))

    # Move fetch offset ahead of 300 message (out of range)
    simple_consumer.seek(300, 2)
    # Since auto_offset_reset is set to largest we should not read any
    # messages.
    assert_message_count([message for message in simple_consumer], 0)
    # Send 200 new messages to the queue
    send_messages(simple_client, topic, 0, range(200, 300))
    send_messages(simple_client, topic, 1, range(300, 400))
    # Since the offset is set to largest we should read all the new messages.
    assert_message_count([message for message in simple_consumer], 200)

@set_params(simple_consumer={'auto_offset_reset':None})
def test_simple_consumer_no_reset(simple_client, simple_consumer, topic):
    send_messages(simple_client, topic, 0, range(0, 100))
    send_messages(simple_client, topic, 1, range(100, 200))

    # Move fetch offset ahead of 300 message (out of range)
    simple_consumer.seek(300, 2)
    with pytest.raises(OffsetOutOfRangeError):
        simple_consumer.get_message()

@kafka_versions('>=0.8.1')
def test_simple_consumer_load_initial_offsets(simple_client, topic):
    send_messages(simple_client, topic, 0, range(0, 100))
    send_messages(simple_client, topic, 1, range(100, 200))

    # Create 1st consumer and change offsets
    group_id = 'test_simple_consumer_load_initial_offsets'
    consumer = get_simple_consumer(simple_client, topic, group=group_id)
    assert consumer.offsets == {0: 0, 1: 0}
    consumer.offsets.update({0:51, 1:101})

    # Update counter after manual offsets update
    consumer.count_since_commit += 1
    consumer.commit()

    # Create 2nd consumer and check initial offsets
    consumer = get_simple_consumer(simple_client, topic, group=group_id, auto_commit=False)
    assert consumer.offsets == {0: 51, 1: 101}

    consumer.stop()

def test_simple_consumer__seek(simple_client, simple_consumer, topic):
    send_messages(simple_client, topic, 0, range(0, 100))
    send_messages(simple_client, topic, 1, range(100, 200))

    # Rewind 10 messages from the end
    simple_consumer.seek(-10, 2)
    assert_message_count([ message for message in simple_consumer ], 10)

    # Rewind 13 messages from the end
    simple_consumer.seek(-13, 2)
    assert_message_count([ message for message in simple_consumer ], 13)

    # Set absolute offset
    simple_consumer.seek(100)
    assert_message_count([ message for message in simple_consumer ], 0)
    simple_consumer.seek(100, partition=0)
    assert_message_count([ message for message in simple_consumer ], 0)
    simple_consumer.seek(101, partition=1)
    assert_message_count([ message for message in simple_consumer ], 0)
    simple_consumer.seek(90, partition=0)
    assert_message_count([ message for message in simple_consumer ], 10)
    simple_consumer.seek(20, partition=1)
    assert_message_count([ message for message in simple_consumer ], 80)
    simple_consumer.seek(0, partition=1)
    assert_message_count([ message for message in simple_consumer ], 100)

def test_simple_consumer_blocking(simple_client, simple_consumer, topic):
    # Ask for 5 messages, nothing in queue, block 1 second
    with Timer() as t:
        messages = simple_consumer.get_messages(block=True, timeout=1)
        assert_message_count(messages, 0)
    assert t.interval >= 1

    send_messages(simple_client, topic, 0, range(0, 5))
    send_messages(simple_client, topic, 1, range(5, 10))

    # Ask for 5 messages, 10 in queue. Get 5 back, no blocking
    with Timer() as t:
        messages = simple_consumer.get_messages(count=5, block=True, timeout=3)
        assert_message_count(messages, 5)
    assert t.interval < 3

    # Ask for 10 messages, get 5 back, block 1 second
    with Timer() as t:
        messages = simple_consumer.get_messages(count=10, block=True, timeout=1)
        assert_message_count(messages, 5)
    assert t.interval >= 1

    # Ask for 10 messages, 5 in queue, ask to block for 1 message or 1
    # second, get 5 back, no blocking
    send_messages(simple_client, topic, 0, range(0, 3))
    send_messages(simple_client, topic, 1, range(3, 5))
    with Timer() as t:
        messages = simple_consumer.get_messages(count=10, block=1, timeout=1)
        assert_message_count(messages, 5)
    assert t.interval <= 1

def test_simple_consumer_pending(simple_client, topic):
    # make sure that we start with no pending messages
    simple_consumer = get_simple_consumer(simple_client, topic)
    assert simple_consumer.pending() == 0
    assert simple_consumer.pending(partitions=[0]) == 0
    assert simple_consumer.pending(partitions=[1]) == 0

    # Produce 10 messages to partitions 0 and 1
    send_messages(simple_client, topic, 0, range(0, 10))
    send_messages(simple_client, topic, 1, range(10, 20))
    simple_consumer.stop()

    simple_consumer = get_simple_consumer(simple_client, topic)

    assert simple_consumer.pending() == 20
    assert simple_consumer.pending(partitions=[0]) == 10
    assert simple_consumer.pending(partitions=[1]) == 10

    # move to last message, so one partition should have 1 pending
    # message and other 0
    simple_consumer.seek(-1, 2)
    assert simple_consumer.pending() == 1

    pending_part1 = simple_consumer.pending(partitions=[0])
    pending_part2 = simple_consumer.pending(partitions=[1])
    assert set([0, 1]) == set([pending_part1, pending_part2])
    simple_consumer.stop()

@pytest.mark.skip('MultiProcessConsumer deprecated and these tests are flaky')
@set_params(simple_consumer={'consumer':MultiProcessConsumer})
def test_multi_process_consumer(simple_client, simple_consumer, topic):
    # Produce 100 messages to partitions 0 and 1
    send_messages(simple_client, topic, 0, range(0, 100))
    send_messages(simple_client, topic, 1, range(100, 200))

    assert_message_count([ message for message in simple_consumer ], 200)

@pytest.mark.skip('MultiProcessConsumer deprecated and these tests are flaky')
@set_params(simple_consumer={'consumer':MultiProcessConsumer})
def test_multi_process_consumer_blocking(simple_client, simple_consumer, topic):
    # Ask for 5 messages, No messages in queue, block 1 second
    with Timer() as t:
        messages = simple_consumer.get_messages(block=True, timeout=1)
        assert_message_count(messages, 0)

    assert t.interval >= 1

    # Send 10 messages
    send_messages(simple_client, topic, 0, range(0, 10))

    # Ask for 5 messages, 10 messages in queue, block 0 seconds
    with Timer() as t:
        messages = simple_consumer.get_messages(count=5, block=True, timeout=5)
        assert_message_count(messages, 5)
    assert t.interval <= 1

    # Ask for 10 messages, 5 in queue, block 1 second
    with Timer() as t:
        messages = simple_consumer.get_messages(count=10, block=True, timeout=1)
        assert_message_count(messages, 5)
    assert t.interval >= 1

    # Ask for 10 messages, 5 in queue, ask to block for 1 message or 1
    # second, get at least one back, no blocking
    send_messages(simple_client, topic, 0, range(0, 5))
    with Timer() as t:
        messages = simple_consumer.get_messages(count=10, block=1, timeout=1)
        received_message_count = len(messages)
        assert received_message_count >= 1
        assert_message_count(messages, received_message_count)
    assert t.interval <= 1

@pytest.mark.skip('MultiProcessConsumer deprecated and these tests are flaky')
# set group to None and auto_commit to False to avoid interactions w/
# offset commit/fetch apis
@set_params(simple_consumer={'consumer':MultiProcessConsumer, 'auto_commit':False, 'iter_timeout':0})
def test_multi_proc_pending(simple_client, simple_consumer, topic):
    send_messages(simple_client, topic, 0, range(0, 10))
    send_messages(simple_client, topic, 1, range(10, 20))

    assert simple_consumer.pending() == 20
    assert simple_consumer.pending(partitions=[0]) == 10
    assert simple_consumer.pending(partitions=[1]) == 10

@pytest.mark.skip('MultiProcessConsumer deprecated and these tests are flaky')
@kafka_versions('>=0.8.1')
def test_multi_process_consumer_load_initial_offsets(simple_client, topic):
    send_messages(simple_client, topic, 0, range(0, 10))
    send_messages(simple_client, topic, 1, range(10, 20))

    # Create 1st consumer and change offsets
    group_id = 'test_multi_process_consumer_load_initial_offsets'
    consumer = get_simple_consumer(simple_client, topic, group=group_id)
    assert consumer.offsets == {0: 0, 1: 0}
    consumer.offsets.update({0:5, 1:15})
    # Update counter after manual offsets update
    consumer.count_since_commit += 1
    consumer.commit()

    # Create 2nd consumer and check initial offsets
    consumer = get_simple_consumer(simple_client, topic, group=group_id,
                                                         consumer=MultiProcessConsumer,
                                                         auto_commit=False)
    assert consumer.offsets == {0: 5, 1: 15}

@set_params(simple_consumer={'max_buffer_size':60000})
def test_large_messages(simple_client, simple_consumer, topic):
    # Produce 10 "normal" size messages
    small_messages = send_messages(simple_client, topic, 0, [ str(x) for x in range(10) ])

    # Produce 10 messages that are large (bigger than default fetch size)
    large_messages = send_messages(simple_client, topic, 0, [ random_string(5000) for x in range(10) ])

    # Brokers prior to 0.11 will return the next message
    # if it is smaller than max_bytes (called buffer_size in SimpleConsumer)
    # Brokers 0.11 and later that store messages in v2 format
    # internally will return the next message only if the
    # full MessageSet is smaller than max_bytes.
    # For that reason, we set the max buffer size to a little more
    # than the size of all large messages combined
    expected_messages = set(small_messages + large_messages)
    actual_messages = set([ x.message.value for x in simple_consumer ])
    assert expected_messages == actual_messages

def test_huge_messages(simple_client, topic):
    huge_message, = send_messages(simple_client, topic, 0, [
        create_message(random_string(MAX_FETCH_BUFFER_SIZE_BYTES + 10)),
    ])

    # Create a consumer with the default buffer size
    consumer = get_simple_consumer(simple_client, topic)

    # This consumer fails to get the message
    with pytest.raises(ConsumerFetchSizeTooSmall):
        consumer.get_message(False, 0.1)

    consumer.stop()

    # Create a consumer with no fetch size limit
    big_consumer = get_simple_consumer(simple_client, topic, max_buffer_size=None, partitions=[0])

    # Seek to the last message
    big_consumer.seek(-1, 2)

    # Consume giant message successfully
    message = big_consumer.get_message(block=False, timeout=10)
    assert message is not None
    assert message.message.value == huge_message

    big_consumer.stop()

@kafka_versions('>=0.8.1')
def test_offset_behavior__resuming_behavior(simple_client, topic):
    send_messages(simple_client, topic, 0, range(0, 100))
    send_messages(simple_client, topic, 1, range(100, 200))

    # Start a consumer
    group_id = 'test_offset_behavior__resuming_behavior'
    consumer1 = get_simple_consumer(simple_client, topic,
                                    group=group_id,
                                    auto_commit=True,
                                    auto_commit_every_t=None,
                                    auto_commit_every_n=20)

    # Grab the first 195 messages
    output_msgs1 = [ consumer1.get_message().message.value for _ in xrange(195) ]
    assert_message_count(output_msgs1, 195)

    # The total offset across both partitions should be at 180
    consumer2 = get_simple_consumer(simple_client, topic,
                                    group=group_id,
                                    auto_commit=True,
                                    auto_commit_every_t=None,
                                    auto_commit_every_n=20)

    # 181-200
    assert_message_count([ message for message in consumer2 ], 20)

    consumer1.stop()
    consumer2.stop()

@pytest.mark.skip('MultiProcessConsumer deprecated and these tests are flaky')
@kafka_versions('>=0.8.1')
def test_multi_process_offset_behavior__resuming_behavior(simple_client, topic):
    send_messages(simple_client, topic, 0, range(0, 100))
    send_messages(simple_client, topic, 1, range(100, 200))

    # Start a consumer
    group_id = 'test_multi_process_offset_behavior__resuming_behavior'
    consumer1 = get_simple_consumer(simple_client, topic,
                                    consumer=MultiProcessConsumer,
                                    group=group_id,
                                    auto_commit=True,
                                    auto_commit_every_t=None,
                                    auto_commit_every_n=20)

    # Grab the first 195 messages
    output_msgs1 = []
    idx = 0
    for message in consumer1:
        output_msgs1.append(message.message.value)
        idx += 1
        if idx >= 195:
            break
    assert_message_count(output_msgs1, 195)

    # The total offset across both partitions should be at 180
    consumer2 = get_simple_consumer(simple_client, topic,
                                    consumer=MultiProcessConsumer,
                                    group=group_id,
                                    auto_commit=True,
                                    auto_commit_every_t=None,
                                    auto_commit_every_n=20)

    # 181-200
    assert_message_count([ message for message in consumer2 ], 20)

    consumer1.stop()
    consumer2.stop()

# TODO: Make this a unit test -- should not require integration
@set_params(simple_consumer={'buffer_size':1024,'max_buffer_size':2048})
def test_fetch_buffer_size(simple_client, simple_consumer, topic):

    # Test parameters (see issue 135 / PR 136)
    TEST_MESSAGE_SIZE=1048
    INIT_BUFFER_SIZE=1024
    MAX_BUFFER_SIZE=2048
    assert TEST_MESSAGE_SIZE > INIT_BUFFER_SIZE
    assert TEST_MESSAGE_SIZE < MAX_BUFFER_SIZE
    assert MAX_BUFFER_SIZE == 2 * INIT_BUFFER_SIZE

    send_messages(simple_client, topic, 0, [ "x" * 1048 ])
    send_messages(simple_client, topic, 1, [ "x" * 1048 ])

    messages = [ message for message in simple_consumer ]
    assert len(messages) == 2

