import collections
import logging
import pytest
import six
import threading
import time

from kafka.conn import ConnectionStates
from kafka.structs import TopicPartition
from test.testutil import kafka_versions, random_string

def test_consumer(kafka_consumer):
    kafka_consumer.poll(500)
    assert len(kafka_consumer._client._conns) > 0
    node_id = list(kafka_consumer._client._conns.keys())[0]
    assert kafka_consumer._client._conns[node_id].state is ConnectionStates.CONNECTED

@kafka_versions('>=0.9')
def test_group(kafka_broker, topic):
    threads = {}
    messages = collections.defaultdict(list)
    group_id = 'test-group-' + random_string(6)

    def consumer_thread(i, consumer, stop):
        while not stop.is_set():
            for tp, records in six.itervalues(consumer.poll(100)):
                messages[i][tp].extend(records)
        consumer.close()

    num_partitions = kafka_broker.partitions
    consumers = tuple(kafka_broker.get_consumers(cnt=num_partitions, topics=[topic], group_id=group_id, heartbeat_interval_ms=500))
    stop = tuple(threading.Event() for x in range(num_partitions))
    for i in range(num_partitions):
        t = threading.Thread(target=consumer_thread, args=(i, consumers[i], stop[i]))
        t.start()
        threads[i] = t

    try:
        timeout = time.time() + 35
        while True:
            for c in range(num_partitions):

                # Verify all consumers have an assignment
                if not consumers[c].assignment():
                    break

            # If all consumers exist and have an assignment
            else:

                # Verify all consumers are in the same generation
                # then log state and break while loop
                generations = set([consumer._coordinator.generation
                                   for consumer in consumers])

                # New generation assignment is not complete until
                # coordinator.rejoining = False
                rejoining = any([consumer._coordinator.rejoining
                                 for consumer in consumers])

                if not rejoining and len(generations) == 1:
                    for c, consumer in enumerate(consumers):
                        logging.info("[%s] %s %s: %s", c,
                                     consumer._coordinator.generation,
                                     consumer._coordinator.member_id,
                                     consumer.assignment())
                    break
            assert time.time() < timeout, "timeout waiting for assignments"

        group_assignment = set()
        for c in range(num_partitions):
            assert len(consumers[c].assignment()) != 0
            assert set.isdisjoint(consumers[c].assignment(), group_assignment)
            group_assignment.update(consumers[c].assignment())

        assert group_assignment == set([
            TopicPartition(topic, partition)
                for partition in range(num_partitions)])

    finally:
        for c in range(num_partitions):
            stop[c].set()
            threads[c].join()


def test_paused(kafka_broker, topic):
    try:
        (consumer,) = kafka_broker.get_consumers(cnt=1, topics=[])
        topics = [TopicPartition(topic, 1)]
        consumer.assign(topics)
        assert set(topics) == consumer.assignment()
        assert set() == consumer.paused()

        consumer.pause(topics[0])
        assert set([topics[0]]) == consumer.paused()

        consumer.resume(topics[0])
        assert set() == consumer.paused()

        consumer.unsubscribe()
        assert set() == consumer.paused()
    finally:
        if consumer:
            consumer.close()
