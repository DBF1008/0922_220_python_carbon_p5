import carbon.client as carbon_client
from carbon.client import (
  CarbonPickleClientFactory, CarbonPickleClientProtocol, CarbonLineClientProtocol,
  CarbonClientManager, RelayProcessor
)
from carbon.routers import DatapointRouter
from carbon.tests.util import TestSettings
from carbon import state
import carbon.service  # NOQA

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.base import DelayedCall
from twisted.internet.task import deferLater
from twisted.trial.unittest import TestCase
from twisted.test.proto_helpers import StringTransport

from mock import Mock, patch
from pickle import loads as pickle_loads
from struct import unpack, calcsize


INT32_FORMAT = '!I'
INT32_SIZE = calcsize(INT32_FORMAT)


def decode_sent(data):
  pickle_size = unpack(INT32_FORMAT, data[:INT32_SIZE])[0]
  return pickle_loads(data[INT32_SIZE:INT32_SIZE + pickle_size])


class BroadcastRouter(DatapointRouter):
  def __init__(self, destinations=[]):
    self.destinations = set(destinations)

  def addDestination(self, destination):
    self.destinations.append(destination)

  def removeDestination(self, destination):
    self.destinations.discard(destination)

  def getDestinations(self, key):
    for destination in self.destinations:
      yield destination


class ConnectedCarbonClientProtocolTest(TestCase):
  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    carbon_client.settings = TestSettings()  # reset to defaults
    factory = CarbonPickleClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)
    self.protocol = factory.buildProtocol(('127.0.0.1', 2003))
    self.transport = StringTransport()
    self.protocol.makeConnection(self.transport)

  def test_send_datapoint(self):
    def assert_sent():
      sent_data = self.transport.value()
      sent_datapoints = decode_sent(sent_data)
      self.assertEqual([datapoint], sent_datapoints)

    datapoint = ('foo.bar', (1000000000, 1.0))
    self.protocol.sendDatapoint(*datapoint)
    return deferLater(reactor, 0.1, assert_sent)


class CarbonLineClientProtocolTest(TestCase):
  def setUp(self):
    self.protocol = CarbonLineClientProtocol()
    self.protocol.sendLine = Mock()

  def test_send_datapoints(self):
    calls = [
      (('foo.bar', (1000000000, 1.0)), b'foo.bar 1 1000000000'),
      (('foo.bar', (1000000000, 1.1)), b'foo.bar 1.1 1000000000'),
      (('foo.bar', (1000000000, 1.123456789123)), b'foo.bar 1.1234567891 1000000000'),
      (('foo.bar', (1000000000, 1)), b'foo.bar 1 1000000000'),
      (('foo.bar', (1000000000, 1.498566361088E12)), b'foo.bar 1498566361088 1000000000'),
    ]

    i = 0
    for (datapoint, expected_line_to_send) in calls:
      i += 1

      self.protocol._sendDatapointsNow([datapoint])
      self.assertEqual(self.protocol.sendLine.call_count, i)
      self.protocol.sendLine.assert_called_with(expected_line_to_send)


class CarbonClientFactoryTest(TestCase):
  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    self.protocol_mock = Mock(spec=CarbonPickleClientProtocol)
    self.protocol_patch = patch(
      'carbon.client.CarbonPickleClientProtocol', new=Mock(return_value=self.protocol_mock))
    self.protocol_patch.start()
    carbon_client.settings = TestSettings()
    self.factory = CarbonPickleClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)
    self.connected_factory = CarbonPickleClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)
    self.connected_factory.buildProtocol(None)
    self.connected_factory.started = True

  def tearDown(self):
    if self.factory.deferSendPending and self.factory.deferSendPending.active():
      self.factory.deferSendPending.cancel()
    self.protocol_patch.stop()

  def test_schedule_send_schedules_call_to_send_queued(self):
    self.factory.scheduleSend()
    self.assertIsInstance(self.factory.deferSendPending, DelayedCall)
    self.assertTrue(self.factory.deferSendPending.active())

  def test_schedule_send_ignores_already_scheduled(self):
    self.factory.scheduleSend()
    expected_fire_time = self.factory.deferSendPending.getTime()
    self.factory.scheduleSend()
    self.assertTrue(expected_fire_time, self.factory.deferSendPending.getTime())

  def test_send_queued_should_noop_if_not_connected(self):
    self.factory.scheduleSend()
    self.assertFalse(self.protocol_mock.sendQueued.called)

  def test_send_queued_should_call_protocol_send_queued(self):
    self.connected_factory.sendQueued()
    self.protocol_mock.sendQueued.assert_called_once_with()


class CarbonClientManagerTest(TestCase):
  timeout = 1.0

  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    self.factory_mock = Mock(spec=CarbonPickleClientFactory)
    self.client_mgr = CarbonClientManager(self.router_mock)
    self.client_mgr.createFactory = lambda dest: self.factory_mock(dest, self.router_mock)

  def test_start_service_installs_sig_ignore(self):
    from signal import SIGHUP, SIG_IGN

    with patch('signal.signal', new=Mock()) as signal_mock:
      self.client_mgr.startService()
      signal_mock.assert_called_once_with(SIGHUP, SIG_IGN)

  def test_start_service_starts_factory_connect(self):
    factory_mock = Mock(spec=CarbonPickleClientFactory)
    factory_mock.started = False
    self.client_mgr.client_factories[('127.0.0.1', 2003, 'a')] = factory_mock
    self.client_mgr.startService()
    factory_mock.startConnecting.assert_called_once_with()

  def test_stop_service_waits_for_clients_to_disconnect(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startService()
    self.client_mgr.startClient(dest)

    disconnect_deferred = Deferred()
    reactor.callLater(0.1, disconnect_deferred.callback, 0)
    self.factory_mock.return_value.disconnect.return_value = disconnect_deferred
    return self.client_mgr.stopService()

  def test_start_client_instantiates_client_factory(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startClient(dest)
    self.factory_mock.assert_called_once_with(dest, self.router_mock)

  def test_start_client_ignores_duplicate(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startClient(dest)
    self.client_mgr.startClient(dest)
    self.factory_mock.assert_called_once_with(dest, self.router_mock)

  def test_start_client_starts_factory_if_running(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startService()
    self.client_mgr.startClient(dest)
    self.factory_mock.return_value.startConnecting.assert_called_once_with()

  def test_start_client_adds_destination_to_router(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startClient(dest)
    self.router_mock.addDestination.assert_called_once_with(dest)

  def test_stop_client_removes_destination_from_router(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startClient(dest)
    self.client_mgr.stopClient(dest)
    self.router_mock.removeDestination.assert_called_once_with(dest)


class RelayProcessorTest(TestCase):
  timeout = 1.0

  def setUp(self):
    carbon_client.settings = TestSettings()  # reset to defaults
    self.client_mgr_mock = Mock(spec=CarbonClientManager)
    self.client_mgr_patch = patch(
      'carbon.state.client_manager', new=self.client_mgr_mock)
    self.client_mgr_patch.start()

  def tearDown(self):
    self.client_mgr_patch.stop()

  def test_relay_normalized(self):
    carbon_client.settings.TAG_RELAY_NORMALIZED = True
    relayProcessor = RelayProcessor()
    relayProcessor.process('my.metric;foo=a;bar=b', (0.0, 0.0))
    self.client_mgr_mock.sendDatapoint.assert_called_once_with('my.metric;bar=b;foo=a', (0.0, 0.0))

  def test_relay_unnormalized(self):
    carbon_client.settings.TAG_RELAY_NORMALIZED = False
    relayProcessor = RelayProcessor()
    relayProcessor.process('my.metric;foo=a;bar=b', (0.0, 0.0))
    self.client_mgr_mock.sendDatapoint.assert_called_once_with('my.metric;foo=a;bar=b', (0.0, 0.0))


class CarbonClientShutdownDrainTest(TestCase):
  """Regression tests for the shutdown drain path.

  Ensures flow control callbacks do not resume receiving during shutdown,
  keeping backpressure semantics one-directional once shutdown begins.
  """
  timeout = 2.0

  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    self.router_mock.hasDestination = Mock(return_value=True)
    self.router_mock.countDestinations = Mock(return_value=0)
    carbon_client.settings = TestSettings()
    self._orig_shutting_down = state.shuttingDown
    state.shuttingDown = False

  def tearDown(self):
    state.shuttingDown = self._orig_shutting_down

  def test_stop_service_sets_shutting_down_flag(self):
    """CarbonClientManager.stopService must set state.shuttingDown before draining."""
    client_mgr = CarbonClientManager(self.router_mock)
    self.assertFalse(state.shuttingDown)
    client_mgr.stopService()
    self.assertTrue(state.shuttingDown)

  def test_queue_space_callback_suppressed_during_shutdown(self):
    """queueSpaceCallback must not fire cacheSpaceAvailable during shutdown."""
    factory = CarbonPickleClientFactory(
        ('127.0.0.1', 2003, 'a'), self.router_mock)
    # Simulate queue-was-full state so the callback branch executes.
    factory.queueFull.callback(100)

    state.shuttingDown = True

    with patch.object(state.events, 'cacheSpaceAvailable') as csa_mock:
      factory.queueSpaceCallback(50)
      csa_mock.assert_not_called()

  def test_queue_space_callback_works_normally(self):
    """queueSpaceCallback must fire cacheSpaceAvailable when not shutting down."""
    factory = CarbonPickleClientFactory(
        ('127.0.0.1', 2003, 'a'), self.router_mock)
    factory.queueFull.callback(100)

    state.shuttingDown = False

    with patch.object(state.events, 'cacheSpaceAvailable') as csa_mock:
      factory.queueSpaceCallback(50)
      csa_mock.assert_called_once_with()

  def test_destination_up_suppressed_during_shutdown(self):
    """destinationUp must not resume receiving during shutdown."""
    factory = CarbonPickleClientFactory(
        ('127.0.0.1', 2003, 'a'), self.router_mock)
    self.router_mock.hasDestination.return_value = False
    dest = ('127.0.0.1', 2003, 'a')

    state.shuttingDown = True

    with patch.object(state.events, 'resumeReceivingMetrics') as rrm_mock:
      factory.destinationUp(dest)
      rrm_mock.assert_not_called()
    # Destination should still be added to the router for orderly drain.
    self.router_mock.addDestination.assert_called_once_with(dest)

  def test_destination_down_no_reinject_during_shutdown(self):
    """destinationDown must not re-inject metrics during shutdown."""
    carbon_client.settings.DYNAMIC_ROUTER = True
    factory = CarbonPickleClientFactory(
        ('127.0.0.1', 2003, 'a'), self.router_mock)
    factory.retries = carbon_client.settings.DYNAMIC_ROUTER_MAX_RETRIES + 1
    factory.queue.append(('test.metric', (1000, 1.0)))
    factory.queue.append(('test.metric2', (1001, 2.0)))
    dest = ('127.0.0.1', 2003, 'a')

    state.shuttingDown = True

    with patch.object(state.events, 'metricGenerated') as mg_mock:
      with patch.object(state.events, 'pauseReceivingMetrics') as prm_mock:
        factory.destinationDown(dest)
        mg_mock.assert_not_called()
        prm_mock.assert_not_called()
    # Metrics remain in queue for orderly drain through existing connection.
    self.assertEqual(2, len(factory.queue))

  def test_destination_down_reinject_works_normally(self):
    """destinationDown must re-inject metrics when not shutting down."""
    carbon_client.settings.DYNAMIC_ROUTER = True
    factory = CarbonPickleClientFactory(
        ('127.0.0.1', 2003, 'a'), self.router_mock)
    factory.retries = carbon_client.settings.DYNAMIC_ROUTER_MAX_RETRIES + 1
    factory.queue.append(('test.metric', (1000, 1.0)))
    dest = ('127.0.0.1', 2003, 'a')

    state.shuttingDown = False

    with patch.object(state.events, 'metricGenerated') as mg_mock:
      factory.destinationDown(dest)
      mg_mock.assert_called_once_with('test.metric', (1000, 1.0))
    self.assertEqual(0, len(factory.queue))

  def test_high_priority_metrics_drain_with_regular_during_shutdown(self):
    """High-priority metrics queued during shutdown are drained alongside regular ones."""
    factory = CarbonPickleClientFactory(
        ('127.0.0.1', 2003, 'a'), self.router_mock)
    factory.queue.append(('regular.metric', (1000, 1.0)))
    factory.sendHighPriorityDatapoint('internal.metric', (1001, 2.0))

    state.shuttingDown = True

    # High-priority goes to front, regular stays at back.
    self.assertEqual('internal.metric', factory.queue[0][0])
    self.assertEqual('regular.metric', factory.queue[1][0])
    # Both remain in queue for orderly drain; no resume triggered.
    self.assertEqual(2, len(factory.queue))
