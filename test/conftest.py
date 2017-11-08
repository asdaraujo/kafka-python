from __future__ import absolute_import

import inspect
import pytest
import time

from decorator import decorator, decorate

from kafka import MultiProcessConsumer, SimpleConsumer
from test.fixtures import KafkaFixture, ZookeeperFixture, get_simple_consumer, get_simple_producer, get_keyed_producer, get_base_producer, get_simple_client
from test.testutil import kafka_version, version_str_to_list, random_string, wait_for_kafka_client_topic_update, wait_for_simple_client_topic_update

@pytest.fixture(scope="module")
def version():
    return kafka_version()

@pytest.fixture(scope="module")
def zookeeper():
    zk = ZookeeperFixture.instance()
    yield zk
    zk.close()

_params = {}
def _set_params(func, *args, **kw):
    ret = func(*args, **kw)
    return ret

def set_params(**params):
    global _params

    def real_set_params(func, *args, **kwargs):
        _params[func.__name__] = params
        ret = decorate(func, _set_params)
        return ret

    return real_set_params

# this must be called from the module where the broker is used
def set_broker_params(**params):
    global _params
    caller = inspect.stack()[1]
    if isinstance(caller, tuple):
        filename = caller[1]
    else:
        filename = inspect.stack()[1].filename
    _params[filename] = params

@pytest.fixture(scope='module')
def broker_params(request):
    global _params
    return _params[request.node.fspath] if request.node.fspath in _params else None

@pytest.fixture
def simple_consumer_params(request):
    return _get_params(request)

@pytest.fixture
def base_producer_params(request):
    return _get_params(request)

@pytest.fixture
def simple_producer_params(request):
    return _get_params(request)

@pytest.fixture
def keyed_producer_params(request):
    return _get_params(request)

@pytest.fixture
def kafka_consumer_params(request):
    return _get_params(request)

@pytest.fixture
def kafka_producer_params(request):
    return _get_params(request)

def _get_params(request):
    global _params
    if request._pyfuncitem.name in _params and request.fixturename.replace('_params','') in _params[request._pyfuncitem.name]:
        return _params[request._pyfuncitem.name][request.fixturename.replace('_params','')]
    else:
        return None

@pytest.fixture(scope="module")
def kafka_broker(kafka_brokers):
    return kafka_brokers[0]

@pytest.fixture(scope="module")
def kafka_brokers(version, zookeeper, broker_params):
    assert version, 'KAFKA_VERSION must be specified to run integration tests'
    params = {} if broker_params is None else broker_params.copy()
    params.setdefault('partitions', 2)
    num_brokers = params.pop('num_brokers', 1)
    brokers = tuple(KafkaFixture.instance(x, zookeeper.host, zookeeper.port, **params) for x in range(num_brokers))
    yield brokers
    for broker in brokers:
        broker.close()

@pytest.fixture
def simple_client(kafka_broker, request, topic):
    s = time.time()
    client = get_simple_client(kafka_broker, client_id='%s_client' % (request.node.name,))
    client.ensure_topic_exists(topic)
    yield client
    client.close()

@pytest.fixture
def simple_consumer(version, simple_client, topic, simple_consumer_params):
    params = {} if simple_consumer_params is None else simple_consumer_params.copy()
    consumer = get_simple_consumer(simple_client, topic, **params)
    yield consumer
    consumer.stop()

@pytest.fixture
def base_producer(version, simple_client, topic, base_producer_params):
    params = {} if base_producer_params is None else base_producer_params.copy()
    producer = get_base_producer(simple_client, **params)
    yield producer
    producer.stop()

@pytest.fixture
def simple_producer(version, simple_client, topic, simple_producer_params):
    params = {} if simple_producer_params is None else simple_producer_params.copy()
    producer = get_simple_producer(simple_client, **params)
    yield producer
    producer.stop()

@pytest.fixture
def keyed_producer(version, simple_client, topic, keyed_producer_params):
    params = {} if keyed_producer_params is None else keyed_producer_params.copy()
    producer = get_keyed_producer(simple_client, **params)
    yield producer
    producer.stop()

@pytest.fixture
def kafka_client(kafka_broker, request):
    s = time.time()
    (client,) = kafka_broker.get_clients(cnt=1, client_id='%s_client' % (request.node.name,))
    yield client
    client.close()

@pytest.fixture
def kafka_consumer(kafka_broker, topic, request, kafka_consumer_params):
    params = {} if kafka_consumer_params is None else kafka_consumer_params.copy()
    params.setdefault('client_id', 'consumer_%s' % (request.node.name,))
    (consumer,) = kafka_broker.get_consumers(cnt=1, topics=[topic], **params)
    yield consumer
    consumer.close()

@pytest.fixture
def kafka_producer(kafka_broker, request, kafka_producer_params):
    params = {} if kafka_producer_params is None else kafka_producer_params.copy()
    params.setdefault('client_id', 'producer_%s' % (request.node.name,))
    (producer,) = kafka_broker.get_producers(cnt=1, **params)
    yield producer
    producer.close()

@pytest.fixture
def topic(version, kafka_broker, request):
    s = time.time()
    topic_name = '%s_%s' % (request.node.name, random_string(10))
    kafka_broker.create_topics([topic_name])
    return topic_name

@pytest.fixture
def conn(mocker):
    from kafka.conn import ConnectionStates
    from kafka.future import Future
    from kafka.protocol.metadata import MetadataResponse
    conn = mocker.patch('kafka.client_async.BrokerConnection')
    conn.return_value = conn
    conn.state = ConnectionStates.CONNECTED
    conn.send.return_value = Future().success(
        MetadataResponse[0](
            [(0, 'foo', 12), (1, 'bar', 34)],  # brokers
            []))  # topics
    conn.blacked_out.return_value = False
    def _set_conn_state(state):
        conn.state = state
        return state
    conn._set_conn_state = _set_conn_state
    conn.connect.side_effect = lambda: conn.state
    conn.connecting = lambda: conn.state in (ConnectionStates.CONNECTING,
                                             ConnectionStates.HANDSHAKE)
    conn.connected = lambda: conn.state is ConnectionStates.CONNECTED
    conn.disconnected = lambda: conn.state is ConnectionStates.DISCONNECTED
    return conn
