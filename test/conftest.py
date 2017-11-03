from __future__ import absolute_import

import os

import pytest

from test.fixtures import KafkaFixture, ZookeeperFixture

def version_str_to_list(s):
    return tuple(map(int, s.split('.'))) # e.g., [0, 8, 1, 1]

@pytest.fixture(scope="module")
def version():
    if 'KAFKA_VERSION' not in os.environ:
        return ()
    return version_str_to_list(os.environ['KAFKA_VERSION'])

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
