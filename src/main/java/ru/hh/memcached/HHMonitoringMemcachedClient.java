package ru.hh.memcached;

import com.timgroup.statsd.StatsDClient;
import static java.util.Arrays.asList;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import static ru.hh.memcached.HHSpyMemcachedClient.getKey;
import ru.hh.metrics.CounterAggregator;
import ru.hh.metrics.PercentileAggregator;
import ru.hh.metrics.StatsDSender;
import ru.hh.metrics.Tag;

class HHMonitoringMemcachedClient implements HHMemcachedClient {
  public static final Tag HIT_TAG = new Tag("hitMiss", "hit");
  public static final Tag MISS_TAG = new Tag("hitMiss", "miss");

  private final HHMemcachedClient hhMemcachedClient;
  private final CounterAggregator counterAggregator;
  private final PercentileAggregator percentileAggregator;

  HHMonitoringMemcachedClient(HHMemcachedClient hhMemcachedClient, StatsDClient statsDClient,
                              ScheduledExecutorService scheduledExecutorService) {

    this.hhMemcachedClient = hhMemcachedClient;

    counterAggregator = new CounterAggregator(500);
    percentileAggregator = new PercentileAggregator(1000, asList(0.5, 0.97, 0.99, 1.0), 20);

    StatsDSender statsDSender = new StatsDSender(statsDClient, scheduledExecutorService);
    statsDSender.sendCounterPeriodically("memcached.hitMiss", counterAggregator);
    statsDSender.sendPercentilesPeriodically("memcached.time", percentileAggregator);
  }

  @Override
  public Object get(String region, String key) {
    return callSyncWithExecutionTimeAndHitMissStats(() -> hhMemcachedClient.get(region, key), region, key);
  }

  @Override
  public Map<String, Object> getSome(String region, String[] keys) {
    final long startTime = System.currentTimeMillis();
    Map<String, Object> object = hhMemcachedClient.getSome(region, keys);
    final long timeEnd = System.currentTimeMillis();

    for (String key : keys) {
      sendExecutionTimeStats(region, key, startTime, timeEnd);
      sendHitMissStats(object.get(key), region, key);
    }

    return object;
  }

  @Override
  public CompletableFuture<Boolean> set(String region, String key, int exp, Object o) {
    return callAsyncWithExecutionTimeStats(() -> hhMemcachedClient.set(region, key, exp, o), region, key);
  }

  @Override
  public CompletableFuture<Boolean> delete(String region, String key) {
    return callAsyncWithExecutionTimeStats(() -> hhMemcachedClient.delete(region, key), region, key);
  }

  @Override
  public CASPair gets(String region, String key) {
    return callSyncWithExecutionTimeAndHitMissStats(() -> hhMemcachedClient.gets(region, key), region, key);
  }

  @Override
  public CompletableFuture<Boolean> add(String region, String key, int exp, Object o) {
    return callAsyncWithExecutionTimeStats(() -> hhMemcachedClient.add(region, key, exp, o), region, key);
  }

  @Override
  public CompletableFuture<CASResponse> asyncCas(
      String region, String key, long casId, int exp, Object o) {

    return callAsyncWithExecutionTimeStats(() -> hhMemcachedClient.asyncCas(region, key, casId, exp, o), region, key);
  }

  @Override
  public long increment(String region, String key, int by, int def) {
    final long time = System.currentTimeMillis();

    Long object = hhMemcachedClient.increment(region, key, by, def);

    sendExecutionTimeStats(region, key, time, System.currentTimeMillis());
    return object;
  }

  private <T> T callSyncWithExecutionTimeAndHitMissStats(Supplier<T> method, String region, String key) {
    final long time = System.currentTimeMillis();
    T object = method.get();

    sendExecutionTimeStats(region, key, time, System.currentTimeMillis());
    sendHitMissStats(object, region, key);

    return object;
  }

  private <T> CompletableFuture<T> callAsyncWithExecutionTimeStats(
      Supplier<CompletableFuture<T>> method, String region, String key) {
    final long time = System.currentTimeMillis();
    CompletableFuture<T> completableFuture = method.get();

    completableFuture.thenRun(() -> sendExecutionTimeStats(region, key, time, System.currentTimeMillis()));
    return completableFuture;
  }

  private void sendHitMissStats(Object object, String region, String key) {
    Tag regionTag = new Tag("region", region);
    Tag primaryNodeTag =  new Tag("primaryNode", getPrimaryNode(region, key));
    if (object == null) {
      counterAggregator.increaseMetric(1, MISS_TAG, regionTag, primaryNodeTag);
    } else {
      counterAggregator.increaseMetric(1, HIT_TAG, regionTag, primaryNodeTag);
    }
  }

  private String getPrimaryNode(String region, String key) {
    return hhMemcachedClient.getPrimaryNodeAddress(getKey(region, key)).getHostString();
  }

  private void sendExecutionTimeStats(String region, String key, long timeStart, long timeEnd) {
    percentileAggregator.increaseMetric((int) (timeEnd - timeStart), new Tag("primaryNode", getPrimaryNode(region, key)));
  }

  @Override
  public InetSocketAddress getPrimaryNodeAddress(String key) {
    return hhMemcachedClient.getPrimaryNodeAddress(key);
  }

}
