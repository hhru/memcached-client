package ru.hh.memcached;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import net.spy.memcached.OperationTimeoutException;
import static ru.hh.memcached.HHSpyMemcachedClient.getKey;
import ru.hh.nab.metrics.Counters;
import ru.hh.nab.metrics.Histograms;
import ru.hh.nab.metrics.StatsDSender;
import ru.hh.nab.metrics.Tag;

class HHMonitoringMemcachedClient implements HHMemcachedClient {
  private static final Tag HIT_TAG = new Tag("hitMiss", "hit");
  private static final Tag MISS_TAG = new Tag("hitMiss", "miss");

  private static final Tag TIMEOUT_ERROR_TAG = new Tag("error_type", "timeout");
  private static final Tag CANCELLATION_ERROR_TAG = new Tag("error_type", "cancellation");
  private static final Tag OTHER_ERROR_TAG = new Tag("error_type", "other");

  private static final Tag GET_COMMAND_TAG = new Tag("command", "get");
  private static final Tag GET_SOME_COMMAND_TAG = new Tag("command", "getSome");
  private static final Tag SET_COMMAND_TAG = new Tag("command", "set");
  private static final Tag DELETE_COMMAND_TAG = new Tag("command", "delete");
  private static final Tag GETS_COMMAND_TAG = new Tag("command", "gets");
  private static final Tag ADD_COMMAND_TAG = new Tag("command", "add");
  private static final Tag ASYNC_CAS_COMMAND_TAG = new Tag("command", "asyncCas");
  private static final Tag INCREMENT_COMMAND_TAG = new Tag("command", "increment");

  private final HHMemcachedClient hhMemcachedClient;
  private final Counters hitMissCounters;
  private final Histograms histograms;
  private final Counters errorCounters;

  HHMonitoringMemcachedClient(HHMemcachedClient hhMemcachedClient, String serviceName, StatsDSender statsDSender, int metricsSendIntervalSec) {
    this.hhMemcachedClient = hhMemcachedClient;

    hitMissCounters = new Counters(500);
    histograms = new Histograms(1000, 20);
    errorCounters = new Counters(500);

    statsDSender.sendPeriodically(() -> {
      statsDSender.sendCounters(getMetricNameWithServiceName(serviceName, "memcached.hitMiss"), hitMissCounters);
      statsDSender.sendHistograms(getMetricNameWithServiceName(serviceName, "memcached.time"), histograms, 95, 99, 100);
      statsDSender.sendCounters(getMetricNameWithServiceName(serviceName, "memcached.errors"), errorCounters);
    }, metricsSendIntervalSec);
  }

  @Override
  public Object get(String region, String key) {
    return callSyncWithStats(() -> hhMemcachedClient.get(region, key), region, key, GET_COMMAND_TAG);
  }

  @Override
  public Map<String, Object> getSome(String region, String[] keys) {
    long startTime = System.currentTimeMillis();
    Map<String, Object> keysToObjects = callWithExceptionStats(() ->
            hhMemcachedClient.getSome(region, keys), region, GET_SOME_COMMAND_TAG, keys);
    long timeEnd = System.currentTimeMillis();

    for (String key : keys) {
      sendExecutionTimeStats(region, key, startTime, timeEnd);
      sendHitMissStats(keysToObjects.get(key), region, key);
    }

    return keysToObjects;
  }

  @Override
  public CompletableFuture<Boolean> set(String region, String key, int exp, Object o) {
    return callAsyncWithStats(() -> hhMemcachedClient.set(region, key, exp, o), region, key, SET_COMMAND_TAG);
  }

  @Override
  public CompletableFuture<Boolean> delete(String region, String key) {
    return callAsyncWithStats(() -> hhMemcachedClient.delete(region, key), region, key, DELETE_COMMAND_TAG);
  }

  @Override
  public CASPair gets(String region, String key) {
    return callSyncWithStats(() -> hhMemcachedClient.gets(region, key), region, key, GETS_COMMAND_TAG);
  }

  @Override
  public CompletableFuture<Boolean> add(String region, String key, int exp, Object o) {
    return callAsyncWithStats(() -> hhMemcachedClient.add(region, key, exp, o), region, key, ADD_COMMAND_TAG);
  }

  @Override
  public CompletableFuture<CASResponse> asyncCas(
      String region, String key, long casId, int exp, Object o) {

    return callAsyncWithStats(() -> hhMemcachedClient.asyncCas(region, key, casId, exp, o), region, key, ASYNC_CAS_COMMAND_TAG);
  }

  @Override
  public long increment(String region, String key, int by, int def) {
    long time = System.currentTimeMillis();

    long object = callWithExceptionStats(() -> hhMemcachedClient.increment(region, key, by, def), region, INCREMENT_COMMAND_TAG, key);

    sendExecutionTimeStats(region, key, time, System.currentTimeMillis());
    return object;
  }

  private <T> T callSyncWithStats(Supplier<T> method, String region, String key, Tag commandTag) {
    long time = System.currentTimeMillis();
    T object = callWithExceptionStats(method, region, commandTag, key);

    sendExecutionTimeStats(region, key, time, System.currentTimeMillis());
    sendHitMissStats(object, region, key);

    return object;
  }

  private <T> CompletableFuture<T> callAsyncWithStats(
      Supplier<CompletableFuture<T>> method, String region, String key, Tag commandTag) {
    long time = System.currentTimeMillis();
    CompletableFuture<T> completableFuture = method.get();

    completableFuture.whenComplete((completableFutureValue, exception) -> {
      if (exception == null) {
        sendExecutionTimeStats(region, key, time, System.currentTimeMillis());
      } else {
        sendExceptionStats(region, key, commandTag, exception);
      }
    });

    return completableFuture;
  }

  private void sendHitMissStats(Object object, String region, String key) {
    Tag regionTag = new Tag("region", region);
    Tag primaryNodeTag =  new Tag("primaryNode", getPrimaryNode(region, key));
    if (object == null) {
      hitMissCounters.add(1, MISS_TAG, regionTag, primaryNodeTag);
    } else {
      hitMissCounters.add(1, HIT_TAG, regionTag, primaryNodeTag);
    }
  }

  private <T> T callWithExceptionStats(Supplier<T> method, String region, Tag commandTag, String... keys) {
    try {
      return method.get();
    } catch (RuntimeException e) {
      for (String key : keys) {
        sendExceptionStats(region, key, commandTag, e);
      }
      throw e;
    }
  }

  private void sendExceptionStats(String region, String key, Tag commandTag, Throwable exception) {
    Tag typeOfErrorTag;
    Throwable rootCause = getRootCause(exception);

    if (rootCause instanceof OperationTimeoutException || rootCause instanceof TimeoutException) {
      typeOfErrorTag = TIMEOUT_ERROR_TAG;
    } else if (rootCause instanceof CancellationException) {
      typeOfErrorTag = CANCELLATION_ERROR_TAG;
    } else {
      typeOfErrorTag = OTHER_ERROR_TAG;
    }

    Tag primaryNodeTag =  new Tag("primaryNode", getPrimaryNode(region, key));

    errorCounters.add(1, typeOfErrorTag, primaryNodeTag, commandTag);
  }

  private static Throwable getRootCause(Throwable exception) {
    while (exception.getCause() != null) {
      exception = exception.getCause();
    }
    
    return exception;
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
