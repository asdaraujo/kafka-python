from __future__ import absolute_import

import logging
import os
import os.path
import random
import shutil
import socket
import string
import subprocess
import tempfile
import time
import uuid

from six.moves import urllib, xrange
from six.moves.urllib.parse import urlparse  # pylint: disable=E0611,F0401

from kafka import errors, KafkaConsumer, KafkaProducer, TopicPartition, SimpleClient, SimpleConsumer, SimpleProducer, MultiProcessConsumer, KeyedProducer
from kafka.client_async import KafkaClient
from kafka.producer.base import Producer
from kafka.protocol.admin import CreateTopicsRequest
from kafka.protocol.metadata import MetadataRequest
from test.service import ExternalService, SpawnedService

log = logging.getLogger(__name__)

def random_string(l):
    return "".join(random.choice(string.ascii_letters) for i in xrange(l))

def version_str_to_list(s):
    return tuple(map(int, s.split('.'))) # e.g., [0, 8, 1, 1]

def version():
    if 'KAFKA_VERSION' not in os.environ:
        return ()
    return version_str_to_list(os.environ['KAFKA_VERSION'])

def get_open_port():
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

class Fixture(object):
    kafka_version = os.environ.get('KAFKA_VERSION', '0.11.0.1')
    scala_version = os.environ.get("SCALA_VERSION", '2.8.0')
    project_root = os.environ.get('PROJECT_ROOT', os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    kafka_root = os.environ.get("KAFKA_ROOT", os.path.join(project_root, 'servers', kafka_version, "kafka-bin"))
    ivy_root = os.environ.get('IVY_ROOT', os.path.expanduser("~/.ivy2/cache"))

    @classmethod
    def download_official_distribution(cls,
                                       kafka_version=None,
                                       scala_version=None,
                                       output_dir=None):
        if not kafka_version:
            kafka_version = cls.kafka_version
        if not scala_version:
            scala_version = cls.scala_version
        if not output_dir:
            output_dir = os.path.join(cls.project_root, 'servers', 'dist')

        distfile = 'kafka_%s-%s' % (scala_version, kafka_version,)
        url_base = 'https://archive.apache.org/dist/kafka/%s/' % (kafka_version,)
        output_file = os.path.join(output_dir, distfile + '.tgz')

        if os.path.isfile(output_file):
            log.info("Found file already on disk: %s", output_file)
            return output_file

        # New tarballs are .tgz, older ones are sometimes .tar.gz
        try:
            url = url_base + distfile + '.tgz'
            log.info("Attempting to download %s", url)
            response = urllib.request.urlopen(url)
        except urllib.error.HTTPError:
            log.exception("HTTP Error")
            url = url_base + distfile + '.tar.gz'
            log.info("Attempting to download %s", url)
            response = urllib.request.urlopen(url)

        log.info("Saving distribution file to %s", output_file)
        with open(output_file, 'w') as output_file_fd:
            output_file_fd.write(response.read())

        return output_file

    @classmethod
    def test_resource(cls, filename):
        return os.path.join(cls.project_root, "servers", cls.kafka_version, "resources", filename)

    @classmethod
    def kafka_run_class_args(cls, *args):
        result = [os.path.join(cls.kafka_root, 'bin', 'kafka-run-class.sh')]
        result.extend(map(lambda x: str(x), args))
        return result

    def kafka_run_class_env(self):
        env = os.environ.copy()
        env['KAFKA_LOG4J_OPTS'] = "-Dlog4j.configuration=file:%s" % self.test_resource("log4j.properties")
        return env

    @classmethod
    def render_template(cls, source_file, target_file, binding):
        log.info('Rendering %s from template %s', target_file, source_file)
        with open(source_file, "r") as handle:
            template = handle.read()
            assert len(template) > 0, 'Empty template %s' % source_file
        with open(target_file, "w") as handle:
            handle.write(template.format(**binding))
            handle.flush()
            os.fsync(handle)

        # fsync directory for durability
        # https://blog.gocept.com/2013/07/15/reliable-file-updates-with-python/
        dirfd = os.open(os.path.dirname(target_file), os.O_DIRECTORY)
        os.fsync(dirfd)
        os.close(dirfd)


class ZookeeperFixture(Fixture):
    @classmethod
    def instance(cls):
        if "ZOOKEEPER_URI" in os.environ:
            parse = urlparse(os.environ["ZOOKEEPER_URI"])
            (host, port) = (parse.hostname, parse.port)
            fixture = ExternalService(host, port)
        else:
            (host, port) = ("127.0.0.1", None)
            fixture = cls(host, port)

        fixture.open()
        return fixture

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.tmp_dir = None
        self.child = None

    def kafka_run_class_env(self):
        env = super(ZookeeperFixture, self).kafka_run_class_env()
        env['LOG_DIR'] = os.path.join(self.tmp_dir, 'logs')
        return env

    def out(self, message):
        log.info("*** Zookeeper [%s:%s]: %s", self.host, self.port or '(auto)', message)

    def open(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.out("Running local instance...")
        log.info("  host    = %s", self.host)
        log.info("  port    = %s", self.port or '(auto)')
        log.info("  tmp_dir = %s", self.tmp_dir)

        # Configure Zookeeper child process
        template = self.test_resource("zookeeper.properties")
        properties = os.path.join(self.tmp_dir, "zookeeper.properties")
        args = self.kafka_run_class_args("org.apache.zookeeper.server.quorum.QuorumPeerMain", properties)
        env = self.kafka_run_class_env()

        # Party!
        timeout = 5
        max_timeout = 60
        backoff = 1
        end_at = time.time() + max_timeout
        tries = 1
        auto_port = (self.port is None)
        while time.time() < end_at:
            if auto_port:
                self.port = get_open_port()
            self.out('Attempting to start on port %d (try #%d)' % (self.port, tries))
            self.render_template(template, properties, vars(self))
            self.child = SpawnedService(args, env)
            self.child.start()
            timeout = min(timeout, max(end_at - time.time(), 0))
            if self.child.wait_for(r"binding to port", timeout=timeout):
                break
            self.child.dump_logs()
            self.child.stop()
            timeout *= 2
            time.sleep(backoff)
            tries += 1
        else:
            raise Exception('Failed to start Zookeeper before max_timeout')
        self.out("Done!")

    def close(self):
        if self.child is None:
            return
        self.out("Stopping...")
        self.child.stop()
        self.child = None
        self.out("Done!")
        shutil.rmtree(self.tmp_dir)

    def __del__(self):
        self.close()


class KafkaFixture(Fixture):
    @classmethod
    def instance(cls, broker_id, zk_host, zk_port, zk_chroot=None,
                 host=None, port=None,
                 transport='PLAINTEXT', replicas=1, partitions=2,
                 sasl_mechanism='PLAIN', auto_create_topic=True):

        if zk_chroot is None:
            zk_chroot = "kafka-python_" + str(uuid.uuid4()).replace("-", "_")
        if "KAFKA_URI" in os.environ:
            parse = urlparse(os.environ["KAFKA_URI"])
            (host, port) = (parse.hostname, parse.port)
            fixture = ExternalService(host, port)
        else:
            # force IPv6 here because of a confusing point:
            #
            #  - if the string "localhost" is passed, Kafka will *only* bind to the IPv4 address of localhost
            #    (127.0.0.1); however, kafka-python will attempt to connect on ::1 and fail
            #
            #  - if the address literal 127.0.0.1 is passed, the metadata request during bootstrap will return
            #    the name "localhost" and we'll go back to the first case. This is odd!
            #
            # Ideally, Kafka would bind to all loopback addresses when we tell it to listen on "localhost" the
            # way it makes an IPv6 socket bound to both 0.0.0.0/0 and ::/0 when we tell it to bind to "" (that is
            # to say, when we make a listener of PLAINTEXT://:port.
            #
            # Note that even though we specify the bind host in bracket notation, Kafka responds to the bootstrap
            # metadata request without square brackets later.
            if host is None:
                host = "[::1]"
            fixture = KafkaFixture(host, port, broker_id,
                                   zk_host, zk_port, zk_chroot,
                                   transport=transport,
                                   replicas=replicas, partitions=partitions,
                                   sasl_mechanism=sasl_mechanism,
                                   auto_create_topic=auto_create_topic)

            fixture.open()
        return fixture

    def __init__(self, host, port, broker_id, zk_host, zk_port, zk_chroot,
                 replicas=1, partitions=2, transport='PLAINTEXT',
                 sasl_mechanism='PLAIN', auto_create_topic=True):

        self.host = host
        self.port = port

        self.broker_id = broker_id
        self.auto_create_topic = auto_create_topic
        self.transport = transport.upper()
        self.sasl_mechanism = sasl_mechanism.upper()
        self.ssl_dir = self.test_resource('ssl')

        # TODO: checking for port connection would be better than scanning logs
        # until then, we need the pattern to work across all supported broker versions
        # The logging format changed slightly in 1.0.0
        self.start_pattern = r"\[Kafka ?Server (id=)?%d\],? started" % broker_id

        self.zk_host = zk_host
        self.zk_port = zk_port
        self.zk_chroot = zk_chroot

        self.replicas = replicas
        self.partitions = partitions

        self.tmp_dir = None
        self.child = None
        self.running = False

        self._client = None

    def bootstrap_server(self):
        return '%s:%d' % (self.host, self.port)

    def kafka_run_class_env(self):
        env = super(KafkaFixture, self).kafka_run_class_env()
        env['LOG_DIR'] = os.path.join(self.tmp_dir, 'logs')
        return env

    def out(self, message):
        log.info("*** Kafka [%s:%s]: %s", self.host, self.port or '(auto)', message.decode('utf-8') if isinstance(message, bytes) else message)

    def open(self):
        if self.running:
            self.out("Instance already running")
            return

        # Create directories
        if self.tmp_dir is None:
            self.tmp_dir = tempfile.mkdtemp()
            os.mkdir(os.path.join(self.tmp_dir, "logs"))
            os.mkdir(os.path.join(self.tmp_dir, "data"))

        self.out("Running local instance...")
        log.info("  host       = %s", self.host)
        log.info("  port       = %s", self.port or '(auto)')
        log.info("  transport  = %s", self.transport)
        log.info("  broker_id  = %s", self.broker_id)
        log.info("  zk_host    = %s", self.zk_host)
        log.info("  zk_port    = %s", self.zk_port)
        log.info("  zk_chroot  = %s", self.zk_chroot)
        log.info("  replicas   = %s", self.replicas)
        log.info("  partitions = %s", self.partitions)
        log.info("  tmp_dir    = %s", self.tmp_dir)

        self.out("Creating Zookeeper chroot node...")
        args = self.kafka_run_class_args("org.apache.zookeeper.ZooKeeperMain",
                                         "-server", "%s:%d" % (self.zk_host, self.zk_port),
                                         "create",
                                         "/%s" % self.zk_chroot,
                                         "kafka-python")
        env = self.kafka_run_class_env()
        proc = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if proc.wait() != 0 or proc.returncode != 0:
            self.out("Failed to create Zookeeper chroot node")
            self.out(proc.stdout.read())
            self.out(proc.stderr.read())
            raise RuntimeError("Failed to create Zookeeper chroot node")
        self.out("Done!")

        # Configure Kafka child process
        properties = os.path.join(self.tmp_dir, "kafka.properties")
        template = self.test_resource("kafka.properties")
        args = self.kafka_run_class_args("kafka.Kafka", properties)
        env = self.kafka_run_class_env()

        timeout = 5
        max_timeout = 60
        backoff = 1
        end_at = time.time() + max_timeout
        tries = 1
        auto_port = (self.port is None)
        while time.time() < end_at:
            # We have had problems with port conflicts on travis
            # so we will try a different port on each retry
            # unless the fixture was passed a specific port
            if auto_port:
                self.port = get_open_port()
            self.out('Attempting to start on port %d (try #%d)' % (self.port, tries))
            self.render_template(template, properties, vars(self))
            self.child = SpawnedService(args, env)
            self.child.start()
            timeout = min(timeout, max(end_at - time.time(), 0))
            if self.child.wait_for(self.start_pattern, timeout=timeout):
                break
            self.child.dump_logs()
            self.child.stop()
            timeout *= 2
            time.sleep(backoff)
            tries += 1
        else:
            raise Exception('Failed to start KafkaInstance before max_timeout')

        (self._client,) = self.get_clients(1, '_internal_client')

        self.out("Done!")
        self.running = True

    def stop(self):
        if not self.running:
            self.out("Instance already stopped")
            return

        self.out("Stopping...")
        self.child.stop()
        self.child = None
        self.running = False
        self.out("Stopped!")

    def close(self):
        self.stop()
        if self.tmp_dir is not None:
            shutil.rmtree(self.tmp_dir)
            self.tmp_dir = None
        self.out("Done!")

    def _send_request(self, request, timeout=None):
        def _failure(e):
            raise e
        retries = 10
        while True:
            node_id = self._client.least_loaded_node()
            for ready_retry in range(40):
                if self._client.ready(node_id, False):
                    break
                time.sleep(.1)
            else:
                raise RuntimeError('Could not connect to broker with node id %d' % (node_id,))

            try:
                future = self._client.send(node_id, request)
                future.error_on_callbacks = True
                future.add_errback(_failure)
                return self._client.poll(future=future, timeout_ms=timeout)
            except Exception as e:
                time.sleep(1)
                retries -= 1
                if retries == 0:
                    raise e
                else:
                    pass # retry

    def _create_topic(self, topic_name, num_partitions, replication_factor, timeout_ms=10000):
        if num_partitions is None:
            num_partitions = self.partitions
        if replication_factor is None:
            replication_factor = self.replicas

        # Try different methods to create a topic, from the fastest to the slowest
        if self.auto_create_topic and num_partitions == self.partitions and replication_factor == self.replicas:
            self._send_request(MetadataRequest[0]([topic_name]))
        elif version() >= (0, 10, 1, 0):
            request = CreateTopicsRequest[0]([(topic_name, num_partitions, replication_factor, [], [])], timeout_ms)
            result = self._send_request(request, timeout=timeout_ms)
            for topic_result in result[0].topic_error_codes:
                error_code = topic_result[1]
                if error_code != 0:
                    raise errors.for_code(error_code)
        else:
            args = self.kafka_run_class_args('kafka.admin.TopicCommand',
                                             '--zookeeper', '%s:%s/%s' % (self.zk_host, self.zk_port, self.zk_chroot),
                                             '--create',
                                             '--topic', topic_name,
                                             '--partitions', self.partitions if num_partitions is None else num_partitions,
                                             '--replication-factor', self.replicas if replication_factor is None else replication_factor)
            if version() >= (0,10):
                args.append('--if-not-exists')
            env = self.kafka_run_class_env()
            proc = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            ret = proc.wait()
            if ret != 0 or proc.returncode != 0:
                output = proc.stdout.read()
                if not 'kafka.common.TopicExistsException' in output:
                    self.out("Failed to create topic %s" % (topic_name,))
                    self.out(output)
                    self.out(proc.stderr.read())
                    raise RuntimeError("Failed to create topic %s" % (topic_name,))

    def create_topics(self, topic_names, num_partitions=None, replication_factor=None):
        for topic_name in topic_names:
            self._create_topic(topic_name, num_partitions, replication_factor)

    def get_clients(self, cnt=1, client_id=None):
        if client_id is None:
            client_id='client'
        return tuple(KafkaClient(client_id='%s_%s' % (client_id, random_string(4)),
                                 bootstrap_servers=self.bootstrap_server()) for x in range(cnt))

    def get_consumers(self, cnt, topics, **params):
        params.setdefault('client_id', 'consumer')
        params.setdefault('heartbeat_interval_ms', 500)
        params['bootstrap_servers'] = self.bootstrap_server()
        client_id = params['client_id']
        for x in range(cnt):
            params['client_id'] = '%s_%s' % (client_id, random_string(4))
            yield KafkaConsumer(*topics, **params)

    def get_producers(self, cnt, **params):
        params.setdefault('client_id', 'producer')
        params['bootstrap_servers'] = self.bootstrap_server()
        client_id = params['client_id']
        for x in range(cnt):
            params['client_id'] = '%s_%s' % (client_id, random_string(4))
            yield KafkaProducer(**params)


def get_simple_client(kafka_broker, **simple_client_params):
    params = simple_client_params.copy()
    params.setdefault('client_id', 'simple_client')
    return SimpleClient(kafka_broker.bootstrap_server(), **params)

def get_simple_consumer(simple_client, topic, **simple_consumer_params):
    params = simple_consumer_params.copy()
    if version() == (0,8,0):
        # Kafka 0.8.0 simply doesn't support offset requests, so hard code it being off
        params['group'] = None
        params['auto_commit'] = False
    else:
        params.setdefault('group', None)
        params.setdefault('auto_commit', False)

    consumer_class = params.pop('consumer', SimpleConsumer)
    group = params.pop('group', None)
    topic = params.pop('topic', topic)

    if consumer_class in [SimpleConsumer, MultiProcessConsumer]:
        params.setdefault('iter_timeout', 0)

    return consumer_class(simple_client, group, topic, **params)

def get_base_producer(simple_client, **base_producer_params):
    params = base_producer_params.copy()
    return Producer(simple_client, **params)

def get_simple_producer(simple_client, **simple_producer_params):
    params = simple_producer_params.copy()
    return SimpleProducer(simple_client, **params)

def get_keyed_producer(simple_client, **keyed_producer_params):
    params = keyed_producer_params.copy()
    return KeyedProducer(simple_client, **params)

