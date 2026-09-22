import carbon.client as carbon_client
from carbon.client import (
  CarbonPickleClientFactory, CarbonPickleClientProtocol, CarbonLineClientProtocol,
  CarbonClientManager, RelayProcessor
)
from carbon import instrumentation
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
from time import time


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


class SendRateTrackingTest(TestCase):
  """Unit tests for the per-connection send rate EWMA and backlog score."""

  def setUp(self):
    carbon_client.settings = TestSettings()
    self.router_mock = Mock(spec=DatapointRouter)
    self.factory = CarbonPickleClientFactory(
        ('127.0.0.1', 2003, 'a'), self.router_mock)

  def test_send_rate_ewma_and_idle_decay(self):
    with patch('carbon.client.time') as time_mock:
      time_mock.return_value = 1000.0
      self.factory.noteDatapointsSent(100)  # first batch only stamps the clock
      self.assertEqual(0.0, self.factory.sendRate)

      time_mock.return_value = 1001.0
      self.factory.noteDatapointsSent(100)  # 100 datapoints over 1 second
      expected = (1.0 - 0.5 ** 0.1) * 100.0
      self.assertAlmostEqual(expected, self.factory.sendRate, places=5)

      # One full EWMA window of idle time halves the observed rate.
      time_mock.return_value = 1011.0
      self.assertAlmostEqual(expected * 0.5, self.factory.sendRate, places=5)

  def test_send_rate_unknown_before_any_send(self):
    self.assertEqual(0.0, self.factory.sendRate)

  def test_backlog_score_estimates_drain_time(self):
    with patch('carbon.client.time') as time_mock:
      time_mock.return_value = 1000.0
      self.factory.noteDatapointsSent(1)
      time_mock.return_value = 1001.0
      self.factory.noteDatapointsSent(100)
      for i in range(50):
        self.factory.enqueue('metric.%d' % i, (0, 1.0))
      self.assertAlmostEqual(
          50.0 / self.factory.sendRate, self.factory.backlogScore, places=5)

  def test_backlog_score_floors_unknown_send_rate(self):
    # Without any observed send rate the score degrades to the queue
    # size (send rate floor of 1 datapoint/second).
    for i in range(50):
      self.factory.enqueue('metric.%d' % i, (0, 1.0))
    self.assertEqual(50.0, self.factory.backlogScore)

  def test_send_datapoints_now_updates_send_rate(self):
    protocol = self.factory.buildProtocol(('127.0.0.1', 2003))
    protocol.makeConnection(StringTransport())
    self.assertIsNone(self.factory._sendRateUpdated)
    protocol.sendDatapointsNow([('foo.bar', (1000000000, 1.0))])
    self.assertIsNotNone(self.factory._sendRateUpdated)


class PooledReplicaSelectionTest(TestCase):
  """Regression tests for backlog-aware replica selection when
  DESTINATION_POOL_REPLICAS is enabled."""

  def setUp(self):
    carbon_client.settings = TestSettings()
    carbon_client.settings.DESTINATION_POOL_REPLICAS = True
    self.router_mock = Mock(spec=DatapointRouter)
    with patch('carbon.client.setUpRandomResolver'):
      self.client_mgr = CarbonClientManager(self.router_mock)
    self.hostport = ('127.0.0.1', 2003)
    self.replicas = {}
    for instance in ('a', 'b', 'c'):
      dest = self.hostport + (instance,)
      factory = CarbonPickleClientFactory(dest, self.router_mock)
      self.replicas[instance] = factory
      self.client_mgr.client_factories[dest] = factory
      self.client_mgr.pooled_factories[self.hostport].add(factory)
    self.router_mock.getDestinations.return_value = [self.hostport + ('a',)]

  def _set_send_rate(self, factory, rate):
    factory._sendRate = rate
    factory._sendRateUpdated = time()

  def _enqueue(self, factory, count):
    for i in range(count):
      factory.enqueue('metric.%d' % i, (0, 1.0))

  def _selected(self):
    factories = self.client_mgr.getFactories('some.metric')
    self.assertEqual(1, len(factories))
    return factories.pop()

  def test_slow_replica_is_avoided(self):
    # Replica 'a' is slow: deep backlog and poor observed send rate.
    self._enqueue(self.replicas['a'], 500)
    self._set_send_rate(self.replicas['a'], 1.0)
    self._set_send_rate(self.replicas['b'], 100.0)
    self._set_send_rate(self.replicas['c'], 100.0)
    for _ in range(5):
      self.assertIn(self._selected(), (
          self.replicas['b'], self.replicas['c']))

  def test_recovered_replica_is_selected_again(self):
    # 'a' was slow but drained its backlog; the other replicas now
    # carry some backlog of their own.
    self._set_send_rate(self.replicas['a'], 100.0)
    for instance in ('b', 'c'):
      self._enqueue(self.replicas[instance], 100)
      self._set_send_rate(self.replicas[instance], 100.0)
    self.assertIs(self.replicas['a'], self._selected())

  def test_send_rate_breaks_queue_size_ties(self):
    # Same queue depth: the replica that actually delivers faster has
    # the lower estimated drain time and must be preferred.
    self._enqueue(self.replicas['a'], 100)
    self._enqueue(self.replicas['b'], 100)
    self._enqueue(self.replicas['c'], 1000)
    self._set_send_rate(self.replicas['a'], 1.0)
    self._set_send_rate(self.replicas['b'], 100.0)
    self._set_send_rate(self.replicas['c'], 100.0)
    self.assertIs(self.replicas['b'], self._selected())

  def test_replicas_balance_load(self):
    # With no send-rate information, assigning a datapoint raises the
    # chosen replica's score, so successive selections rotate through
    # the pool instead of piling onto a single replica.
    chosen = []
    for _ in range(6):
      factory = self._selected()
      chosen.append(factory)
      factory.sendDatapoint('some.metric', (0, 1.0))
    for replica in self.replicas.values():
      self.assertEqual(2, chosen.count(replica))
    for replica in self.replicas.values():
      if replica.deferSendPending and replica.deferSendPending.active():
        replica.deferSendPending.cancel()

  def test_no_destination_buffers_to_fake_factory(self):
    self.router_mock.getDestinations.return_value = []
    self.assertEqual(
        {self.client_mgr.client_factories[None]},
        self.client_mgr.getFactories('some.metric'))


class NonPooledGetFactoriesTest(TestCase):
  """Non-pooled factory resolution must keep its existing semantics."""

  def setUp(self):
    carbon_client.settings = TestSettings()
    carbon_client.settings.DESTINATION_POOL_REPLICAS = False
    self.router_mock = Mock(spec=DatapointRouter)
    self.client_mgr = CarbonClientManager(self.router_mock)
    self.dest = ('127.0.0.1', 2003, 'a')
    self.factory = CarbonPickleClientFactory(self.dest, self.router_mock)
    self.client_mgr.client_factories[self.dest] = self.factory
    self.router_mock.getDestinations.return_value = [self.dest]

  def test_returns_factory_for_destination(self):
    self.assertEqual({self.factory}, self.client_mgr.getFactories('some.metric'))

  def test_buffers_to_fake_factory_when_no_destination(self):
    self.router_mock.getDestinations.return_value = []
    self.assertEqual(
        {self.client_mgr.client_factories[None]},
        self.client_mgr.getFactories('some.metric'))


class PooledConnectionQualityMonitorTest(TestCase):
  """Regression tests for the per-connection quality model used when
  DESTINATION_POOL_REPLICAS is enabled."""

  def setUp(self):
    carbon_client.settings = TestSettings()
    carbon_client.settings.USE_RATIO_RESET = True
    carbon_client.settings.DESTINATION_POOL_REPLICAS = True
    carbon_client.settings.MIN_RESET_STAT_FLOW = 1000
    carbon_client.settings.MIN_RESET_RATIO = 0.9
    self.router_mock = Mock(spec=DatapointRouter)
    self.factory = CarbonPickleClientFactory(
        ('127.0.0.1', 2003, 'a'), self.router_mock)
    self.protocol = self.factory.buildProtocol(('127.0.0.1', 2003))
    self.protocol.makeConnection(StringTransport())

  def tearDown(self):
    if self.factory.deferSendPending and self.factory.deferSendPending.active():
      self.factory.deferSendPending.cancel()

  def _enqueue(self, count):
    for i in range(count):
      self.factory.enqueue('metric.%d' % i, (0, 1.0))

  def _quality(self, sent):
    with patch.dict(instrumentation.prior_stats, {self.protocol.sent: sent}):
      return self.protocol.connectionQualityMonitor()

  def test_healthy_connection_passes(self):
    self.assertTrue(self._quality(sent=10000))

  def test_recovered_connection_passes(self):
    # Backlog mostly drained: 5000 sent vs 100 still queued.
    self._enqueue(100)
    self.assertTrue(self._quality(sent=5000))

  def test_low_workload_passes(self):
    # Not enough per-connection flow to judge quality yet.
    self._enqueue(10)
    self.assertTrue(self._quality(sent=0))

  def test_slow_connection_fails(self):
    self._enqueue(2000)
    self.assertFalse(self._quality(sent=100))

  def test_starved_slow_connection_still_fails(self):
    # Regression: a slow replica stops being assigned new work, so
    # assignment-side counters (attemptedRelays) drop to zero and used
    # to mask the stalled connection.  Its own backlog must still make
    # it eligible for a reset.
    self._enqueue(5000)
    self.assertFalse(self._quality(sent=0))

  def test_send_queued_resets_slow_pooled_connection(self):
    self._enqueue(2000)
    self.protocol.lastResetTime = 0
    with patch.dict(instrumentation.prior_stats, {self.protocol.sent: 10}):
      self.protocol.sendQueued()
    self.assertFalse(self.protocol.connected)

  def test_send_queued_keeps_healthy_pooled_connection(self):
    self._enqueue(100)
    self.protocol.lastResetTime = 0
    with patch.dict(instrumentation.prior_stats, {self.protocol.sent: 10000}):
      self.protocol.sendQueued()
    self.assertTrue(self.protocol.connected)


class NonPooledConnectionQualityMonitorTest(TestCase):
  """The non-pooled quality model must keep its existing semantics:
  per-destination sent is compared against cluster-wide metricsReceived
  and the local queue depth plays no role."""

  def setUp(self):
    carbon_client.settings = TestSettings()
    carbon_client.settings.USE_RATIO_RESET = True
    carbon_client.settings.DESTINATION_POOL_REPLICAS = False
    carbon_client.settings.MIN_RESET_STAT_FLOW = 1000
    carbon_client.settings.MIN_RESET_RATIO = 0.9
    self.router_mock = Mock(spec=DatapointRouter)
    self.factory = CarbonPickleClientFactory(
        ('127.0.0.1', 2003, 'a'), self.router_mock)
    self.protocol = self.factory.buildProtocol(('127.0.0.1', 2003))
    self.protocol.makeConnection(StringTransport())

  def _quality(self, sent, received):
    prior = {self.protocol.sent: sent, 'metricsReceived': received}
    with patch.dict(instrumentation.prior_stats, prior):
      return self.protocol.connectionQualityMonitor()

  def test_good_ratio_passes(self):
    self.assertTrue(self._quality(sent=10000, received=10000))

  def test_bad_ratio_fails(self):
    self.assertFalse(self._quality(sent=100, received=10000))

  def test_queue_depth_is_ignored(self):
    # A deep local queue must not influence the non-pooled decision.
    for i in range(5000):
      self.factory.enqueue('metric.%d' % i, (0, 1.0))
    self.assertTrue(self._quality(sent=10000, received=10000))

  def test_low_flow_passes(self):
    self.assertTrue(self._quality(sent=0, received=10))

  def test_ratio_reset_disabled_passes(self):
    carbon_client.settings.USE_RATIO_RESET = False
    self.assertTrue(self._quality(sent=0, received=100000))
