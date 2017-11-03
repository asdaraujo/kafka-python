import os

from kafka.errors import KafkaTimeoutError
from kafka.protocol import create_message
from kafka.structs import (
    FetchRequestPayload, OffsetCommitRequestPayload, OffsetFetchRequestPayload,
    ProduceRequestPayload)

from test.testutil import KafkaIntegrationStandardTestCase, kafka_versions
from test.conftest import version

class TestKafkaClientIntegration(KafkaIntegrationStandardTestCase):
    num_partitions=2


    #########################
    #   KafkaClient Tests   #
    #########################

    @kafka_versions('>=0.8.1')
    def test_create_delete_filter_topics(self):
        try:
            self.ensure_topics(['topic1', 'topic2', 'topic3'])
            self.client.poll(future=self.client.set_topics(['topic1','topic2']))
            assert set(['topic1', 'topic2']) == set(self.get_topics())
            self.client.poll(future=self.client.add_topic('topic2'))
            assert set(['topic1', 'topic2']) == set(self.get_topics())
            self.client.poll(future=self.client.add_topic('topic4')) # non-existent topic
            assert set(['topic1', 'topic2']) == set(self.get_topics())
            self.client.poll(future=self.client.add_topic('topic3'))
            assert set(['topic1', 'topic2', 'topic3']) == set(self.get_topics())
        finally: 
            self.delete_topics(['topic1', 'topic2', 'topic3'])


    @kafka_versions('>=0.8.1')
    def test_check_version(self):
        inferred_version = self.client.check_version()
        actual_version = tuple(map(lambda x: int(x), version()))
        min_len = min(len(inferred_version), len(actual_version))
        assert inferred_version[:min_len] == actual_version[:min_len]

    @kafka_versions('>=0.8.1')
    def test_connected(self):
        node_id = self.client.least_loaded_node()
        assert self.client.connected(node_id)


