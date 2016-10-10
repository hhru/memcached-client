package ru.hh.memcached;

import com.timgroup.statsd.StatsDClient;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static ru.hh.memcached.HHSpyMemcachedClient.getKey;

class HHMonitoringMemcachedClient extends HHMemcachedDelegateClient {
  private final HHMemcachedClient hhMemcachedClient;
  private final HitMissAggregator hitMissAggregator;
  private final TimeAggregator timeAggregator;

  HHMonitoringMemcachedClient(HHMemcachedClient hhMemcachedClient, StatsDClient statsDClient) {
    super(hhMemcachedClient);
    this.hhMemcachedClient = hhMemcachedClient;

    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread thread = new Thread(r, "memcached_stats_sender");
      thread.setDaemon(true);
      return thread;
    });
    hitMissAggregator = new HitMissAggregator(statsDClient, scheduledExecutorService);
    timeAggregator = new TimeAggregator(statsDClient, scheduledExecutorService);
  }

  @Override
  public Object get(String region, String key) {
    return callSyncWithExecutionTimeAndHitMissStats(() -> hhMemcachedClient.get(region, key), region, key);
  }

  @Override
  public Map<String, Object> getBulk(String region, String[] keys) {
    final long startTime = System.currentTimeMillis();
    Map<String, Object> object = hhMemcachedClient.getBulk(region, keys);
    final long timeEnd = System.currentTimeMillis();

    for (String key : keys) {
      sendExecutionTimeStats(region, key, startTime, timeEnd);
      sendHitMissStats(object.get(key), region);
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
    sendHitMissStats(object, region);

    return object;
  }

  private <T> CompletableFuture<T> callAsyncWithExecutionTimeStats(
      Supplier<CompletableFuture<T>> method, String region, String key) {
    final long time = System.currentTimeMillis();
    CompletableFuture<T> completableFuture = method.get();

    completableFuture.thenRun(() -> sendExecutionTimeStats(region, key, time, System.currentTimeMillis()));
    return completableFuture;
  }

  private void sendHitMissStats(Object object, String region) {
    if (object == null) {
      hitMissAggregator.incrementMiss(region);
    } else {
      hitMissAggregator.incrementHit(region);
    }
  }

  private void sendExecutionTimeStats(String region, String key, long timeStart, long timeEnd) {
    String targetServer = hhMemcachedClient.getServerAddress(getKey(region, key)).getHostString().replace('.', '-');
    timeAggregator.incrementTime(targetServer, (int) (timeEnd - timeStart));
  }

}
