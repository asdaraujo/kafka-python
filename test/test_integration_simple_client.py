import os
import pytest

from kafka.errors import KafkaTimeoutError
from kafka.protocol import create_message
from kafka.structs import (
    FetchRequestPayload, OffsetCommitRequestPayload, OffsetFetchRequestPayload,
    ProduceRequestPayload)
from test.testutil import kafka_versions, wait_for_simple_client_topic_update

def test_consume_none(simple_client, topic):
    fetch = FetchRequestPayload(topic, 0, 0, 1024)

#    wait_for_simple_client_topic_update(simple_client, [topic])
    fetch_resp, = simple_client.send_fetch_request([fetch])
    assert fetch_resp.error == 0
    assert fetch_resp.topic == topic
    assert fetch_resp.partition == 0

    messages = list(fetch_resp.messages)
    assert len(messages) == 0

def test_ensure_topic_exists(simple_client, topic):
    simple_client.ensure_topic_exists(topic, timeout=1)

    # ensure_topic_exists should fail with KafkaTimeoutError
    with pytest.raises(KafkaTimeoutError):
        simple_client.ensure_topic_exists('this_topic_doesnt_exist', timeout=0)

def test_send_produce_request_maintains_request_response_order(simple_client, topic):

    simple_client.ensure_topic_exists('foo')
    simple_client.ensure_topic_exists('bar')

    requests = [
        ProduceRequestPayload(
            'foo', 0,
            [create_message(b'a'), create_message(b'b')]),
        ProduceRequestPayload(
            'bar', 1,
            [create_message(b'a'), create_message(b'b')]),
        ProduceRequestPayload(
            'foo', 1,
            [create_message(b'a'), create_message(b'b')]),
        ProduceRequestPayload(
            'bar', 0,
            [create_message(b'a'), create_message(b'b')]),
    ]

    responses = simple_client.send_produce_request(requests)
    while len(responses):
        request = requests.pop()
        response = responses.pop()
        assert request.topic == response.topic
        assert request.partition == response.partition


####################
#   Offset Tests   #
####################

@kafka_versions('>=0.8.1')
def test_commit_fetch_offsets(simple_client, topic):
    req = OffsetCommitRequestPayload(topic, 0, 42, 'metadata')
    (resp,) = simple_client.send_offset_commit_request('group', [req])
    assert resp.error == 0

    req = OffsetFetchRequestPayload(topic, 0)
    (resp,) = simple_client.send_offset_fetch_request('group', [req])
    assert resp.error == 0
    assert resp.offset == 42
    assert resp.metadata == ''  # Metadata isn't stored for now
