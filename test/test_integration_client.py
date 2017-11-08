import time

from test.testutil import kafka_versions, wait_for_kafka_client_topic_update

@kafka_versions('>=0.8.1')
def test_create_delete_filter_topics(kafka_broker, kafka_client):
    kafka_broker.create_topics(['topic1', 'topic2', 'topic3'])

    kafka_client.poll(future=kafka_client.set_topics(['topic1','topic2']))
    wait_for_kafka_client_topic_update(kafka_client, ['topic1', 'topic2'])

    kafka_client.poll(future=kafka_client.add_topic('topic2'))
    wait_for_kafka_client_topic_update(kafka_client, ['topic1', 'topic2'])

    kafka_client.poll(future=kafka_client.add_topic('topic3'))
    wait_for_kafka_client_topic_update(kafka_client, ['topic1', 'topic2', 'topic3'])

    kafka_client.poll(future=kafka_client.add_topic('topic4')) # non-existent topic
    if kafka_broker.auto_create_topic:
        wait_for_kafka_client_topic_update(kafka_client, ['topic1', 'topic2', 'topic3', 'topic4'])
    else:
        wait_for_kafka_client_topic_update(kafka_client, ['topic1', 'topic2', 'topic3'])

@kafka_versions('>=0.8.1')
def test_check_version(version, kafka_client):
    inferred_version = kafka_client.check_version()
    actual_version = tuple(map(lambda x: int(x), version))
    min_len = min(len(inferred_version), len(actual_version))
    assert inferred_version[:min_len] == actual_version[:min_len]

@kafka_versions('>=0.8.1')
def test_connected(kafka_client):
    node_id = kafka_client.least_loaded_node()
    assert kafka_client.connected(node_id)

