package ru.hh.memcached;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static ru.hh.memcached.HHSpyMemcachedClient.getKey;

/** Catches all exceptions that come from MemcachedClient,<br/>
 *  logs them as WARN without stacktrace,<br/>
 *  returns null / false if needed,<br/>
 *  so upper code does not have to deal with exceptions and can think that ordinary miss happened. */
class HHExceptionSwallowerMemcachedClient implements HHMemcachedClient {

  private static final Logger logger = LoggerFactory.getLogger(HHExceptionSwallowerMemcachedClient.class);

  private final HHMemcachedClient hhMemcachedClient;

  HHExceptionSwallowerMemcachedClient(HHMemcachedClient hhMemcachedClient) {
    this.hhMemcachedClient = hhMemcachedClient;
  }

  @Override
  public Object get(String region, String key) {
    try {
      return hhMemcachedClient.get(region, key);
    } catch (RuntimeException e) {
      logger.warn("get failed, region {}, primary node {}, {}, chain of causes is {}, returning null",
          region, getPrimaryNodeString(region, key), e.toString(), getChainOfCauses(e));
      return null;
  }
    }

  /** @return Map of key to value.<br/>
   * The map does not contain keys that are not in memcached or if an error occurs. **/
  @Override
  public Map<String, Object> getSome(String region, String[] keys) {
    try {
      return hhMemcachedClient.getSome(region, keys);
    } catch (RuntimeException e) {
      logger.warn("getSome failed, region {}, {}, chain of causes is {}, returning empty map",
              region, e.toString(), getChainOfCauses(e));
      return Collections.emptyMap();
    }
  }

  @Override
  public CompletableFuture<Boolean> set(String region, String key, int exp, Object o) {
    CompletableFuture<Boolean> origFuture;
    try {
      origFuture = hhMemcachedClient.set(region, key, exp, o);
    } catch (RuntimeException e) {
      logger.warn("failed to get set future, region {}, primary node {}, {}, chain of causes is {}, returning false future",
          region, getPrimaryNodeString(region, key), e.toString(), getChainOfCauses(e));
      return CompletableFuture.completedFuture(false);
    }

    return getFutureWithoutException(origFuture, false, region, key, "set");
  }

  @Override
  public CompletableFuture<Boolean> delete(String region, String key) {
    CompletableFuture<Boolean> origFuture;
    try {
      origFuture = hhMemcachedClient.delete(region, key);
    } catch (RuntimeException e) {
      logger.warn("failed to get delete future, region {}, primary node {}, {}, chain of causes is {}, returning false future",
          region, getPrimaryNodeString(region, key), e.toString(), getChainOfCauses(e));
      return CompletableFuture.completedFuture(false);
    }
    return getFutureWithoutException(origFuture, false, region, key, "delete");
  }

  @Override
  public CASPair gets(String region, String key) {
    try {
      return hhMemcachedClient.gets(region, key);
    } catch (RuntimeException e) {
      logger.warn("failed to get cas value, region {}, primary node {}, {}, chain of causes is {}, returning null",
          region, getPrimaryNodeString(region, key), e.toString(), getChainOfCauses(e));
      return null;
    }
  }

  @Override
  public CompletableFuture<Boolean> add(String region, String key, int exp, Object o) {
    CompletableFuture<Boolean> origFuture;
    try {
      origFuture = hhMemcachedClient.add(region, key, exp, o);
    } catch (RuntimeException e) {
      logger.warn("failed to get add future, region {}, primary node {}, {}, chain of causes is {}, returning false future",
          region, getPrimaryNodeString(region, key), e.toString(), getChainOfCauses(e));
      return CompletableFuture.completedFuture(false);
    }
    return getFutureWithoutException(origFuture, false, region, key, "add");
  }

  @Override
  public CompletableFuture<CASResponse> asyncCas(String region, String key, long casId, int exp, Object o) {
    CompletableFuture<CASResponse> origFuture;
    try {
      origFuture = hhMemcachedClient.asyncCas(region, key, casId, exp, o);
    } catch (RuntimeException e) {
      logger.warn("failed to get async cas future, region {}, primary node {}, {}, chain of causes is {}, returning error future",
          region, getPrimaryNodeString(region, key), e.toString(), getChainOfCauses(e));
      return CompletableFuture.completedFuture(CASResponse.ERROR);
    }
    return getFutureWithoutException(origFuture, null, region, key, "cas");
  }

  @Override
  public long increment(String region, String key, int by, int def) {
    try {
      return hhMemcachedClient.increment(region, key, by, def);
    } catch (RuntimeException e) {
      logger.warn("failed to increment key value, region {}, primary node {}, {}, chain of causes is {}, returning -1",
          region, getPrimaryNodeString(region, key), e.toString(), getChainOfCauses(e));
      return -1;
    }
  }

  @Override
  public long increment(String region, String key, int by, int def, int ttl) {
    try {
      return hhMemcachedClient.increment(region, key, by, def, ttl);
    } catch (RuntimeException e) {
      logger.warn("failed to increment key value, region {}, primary node {}, {}, chain of causes is {}, TTL is {}, returning -1",
          region, getPrimaryNodeString(region, key), e.toString(), getChainOfCauses(e), ttl);
      return -1;
    }
  }

  @Override
  public InetSocketAddress getPrimaryNodeAddress(String key) {
    return hhMemcachedClient.getPrimaryNodeAddress(key);
  }

  private String getPrimaryNodeString(String region, String key) {
    return getPrimaryNodeAddress(getKey(region, key)).getHostString();
  }

  private <T> CompletableFuture<T> getFutureWithoutException(
          CompletableFuture<T> origFuture,
          T fallback,
          String region,
          String key,
          String method) {
    CompletableFuture<T> completableFuture = origFuture.handle((value, exception) -> {
      if (null == exception) {
        return value;
      } else {
        logger.warn("method async {} failed, causes: {}, region {}, primary node {}, returning {}",
                method, getChainOfCauses(exception), region, getPrimaryNodeString(region, key), fallback);
        return fallback;
      }
    });

    completableFuture.whenComplete((completableFutureValue, exception) -> {
      if (exception instanceof CancellationException) {
        origFuture.cancel(false);
      }
    });

    return completableFuture;
  }

  private String getChainOfCauses(Throwable throwable) {
    StringBuilder stringBuilder = new StringBuilder(throwable.toString());

    while (throwable.getCause() != null) {
      throwable = throwable.getCause();
      stringBuilder.append(" <- ").append(throwable.toString());
    }

    return stringBuilder.toString();
  }
}
