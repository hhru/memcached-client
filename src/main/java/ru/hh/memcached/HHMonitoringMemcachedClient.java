package ru.hh.memcached;


import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import static ru.hh.memcached.HHSpyMemcachedClient.getKey;
import ru.hh.metrics.Counters;
import ru.hh.metrics.Histograms;
import ru.hh.metrics.StatsDSender;
import ru.hh.metrics.Tag;

class HHMonitoringMemcachedClient implements HHMemcachedClient {
  public static final Tag HIT_TAG = new Tag("hitMiss", "hit");
  public static final Tag MISS_TAG = new Tag("hitMiss", "miss");

  private final HHMemcachedClient hhMemcachedClient;
  private final Counters counters;
  private final Histograms histograms;

  HHMonitoringMemcachedClient(HHMemcachedClient hhMemcachedClient, StatsDSender statsDSender, String serviceName) {

    this.hhMemcachedClient = hhMemcachedClient;

    counters = new Counters(500);
    histograms = new Histograms(1000, 20);

    statsDSender.sendCountersPeriodically(getMetricNameWithServiceName(serviceName, "memcached.hitMiss"), counters);
    statsDSender.sendPercentilesPeriodically(getMetricNameWithServiceName(serviceName, "memcached.time"), histograms, 50, 97, 99, 100);
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
      counters.add(1, MISS_TAG, regionTag, primaryNodeTag);
    } else {
      counters.add(1, HIT_TAG, regionTag, primaryNodeTag);
    }
  }

  private String getPrimaryNode(String region, String key) {
    return hhMemcachedClient.getPrimaryNodeAddress(getKey(region, key)).getHostString();
  }

  private void sendExecutionTimeStats(String region, String key, long timeStart, long timeEnd) {
    histograms.save((int) (timeEnd - timeStart), new Tag("primaryNode", getPrimaryNode(region, key)));
  }

  private static String getMetricNameWithServiceName(String serviceName, String metricName) {
    return serviceName + '.' + metricName;
  }

  @Override
  public InetSocketAddress getPrimaryNodeAddress(String key) {
    return hhMemcachedClient.getPrimaryNodeAddress(key);
  }

}
