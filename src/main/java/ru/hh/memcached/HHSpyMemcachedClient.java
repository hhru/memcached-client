package ru.hh.memcached;

import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.OperationFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** Wrapper around spy MemcachedClient.<br/>
 *  Catches all exceptions that come from spy MemcachedClient,<br/>
 *  logs them as WARN without stacktrace,<br/>
 *  returns null / false if needed,<br/>
 *  so upper code does not have to deal with exceptions and can think that ordinary miss happened. */
class HHSpyMemcachedClient implements HHMemcachedClient {

  private static final Logger logger = LoggerFactory.getLogger(HHSpyMemcachedClient.class);

  private final MemcachedClient spyMemcachedClient;

  HHSpyMemcachedClient(MemcachedClient memcachedClient) {
    this.spyMemcachedClient = memcachedClient;
  }

  @Override
  public Object get(String region, String key) {
    String keyWithRegion = getKey(region, key);
    try {
      return spyMemcachedClient.get(keyWithRegion);
    } catch (RuntimeException e) {
      logger.warn("failed to get key, region {}, primary node {}, {}, returning null",
          region, getPrimaryNodeString(keyWithRegion), e.toString());
      return null;
    }
  }

  /** @return Map of key to value.<br/>
   * The map does not contain keys that are not in memcached or if an error occurs. **/
  @Override
  public Map<String, Object> getSome(String region, String[] keys) {
    String[] keysWithRegion = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      keysWithRegion[i] = getKey(region, keys[i]);
    }

    BulkFuture<Map<String, Object>> bulkFuture;
    try {
      bulkFuture = spyMemcachedClient.asyncGetBulk(keysWithRegion);
    } catch (RuntimeException e) {
      logger.warn("failed to get keys future, region {}, {}, returning empty map", region, e.toString());
      return Collections.emptyMap();
    }

    Map<String, Object> keyWithRegionToValue;
    try {
      keyWithRegionToValue = bulkFuture.getSome(spyMemcachedClient.getOperationTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | RuntimeException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      logger.warn("failed to wait keys future, region {}, {}, returning empty map", region, e.toString());
      return Collections.emptyMap();
    }

    Map<String, Object> keyToValue = new HashMap<>(keys.length);
    for (int i = 0; i < keys.length; i++) {
      Object value = keyWithRegionToValue.get(keysWithRegion[i]);
      if(value != null) {
        keyToValue.put(keys[i], value);
      }
    }
    return keyToValue;
  }

  @Override
  public CompletableFuture<Boolean> set(String region, String key, int exp, Object o) {
    String keyWithRegion = getKey(region, key);

    OperationFuture<Boolean> setFuture;
    try {
      setFuture = spyMemcachedClient.set(keyWithRegion, exp, o);
    } catch (RuntimeException e) {
      logger.warn("failed to get set future, region {}, primary node {}, {}",
          region, getPrimaryNodeString(keyWithRegion), e.toString());
      return CompletableFuture.completedFuture(false);
    }

    return getCompletableFutureFromOperationFuture(setFuture, region, false);
  }

  @Override
  public CompletableFuture<Boolean> delete(String region, String key) {
    String keyWithRegion = getKey(region, key);

    OperationFuture<Boolean> deleteFuture;
    try {
      deleteFuture = spyMemcachedClient.delete(keyWithRegion);
    } catch (RuntimeException e) {
      logger.warn("failed to get delete future, region {}, primary node {}, {}",
          region, getPrimaryNodeString(keyWithRegion), e.toString());
      return CompletableFuture.completedFuture(false);
    }

    return getCompletableFutureFromOperationFuture(deleteFuture, region, false);
  }

  @Override
  public CASPair gets(String region, String key) {
    String keyWithRegion = getKey(region, key);

    CASValue<Object> casValue;
    try {
      casValue = spyMemcachedClient.gets(keyWithRegion);
    } catch (RuntimeException e) {
      logger.warn("failed to get cas value, region {}, primary node {}, {}, returning null",
          region, getPrimaryNodeString(keyWithRegion), e.toString());
      return null;
    }

    if (casValue == null) {
      return null;
    }
    return new CASPair<>(casValue.getCas(), casValue.getValue());
  }

  @Override
  public CompletableFuture<Boolean> add(String region, String key, int exp, Object o) {
    String keyWithRegion = getKey(region, key);

    OperationFuture<Boolean> addFuture;
    try {
      addFuture = spyMemcachedClient.add(keyWithRegion, exp, o);
    } catch (RuntimeException e) {
      logger.warn("failed to get add future, region {}, primary node {}, {}",
          region, getPrimaryNodeString(keyWithRegion), e.toString());
      return CompletableFuture.completedFuture(false);
    }

    return getCompletableFutureFromOperationFuture(addFuture, region, false);
  }

  @Override
  public CompletableFuture<CASResponse> asyncCas(String region, String key, long casId, int exp, Object o) {
    String keyWithRegion = getKey(region, key);

    OperationFuture<net.spy.memcached.CASResponse> casResponseFuture;
    try {
      casResponseFuture = spyMemcachedClient.asyncCAS(keyWithRegion, casId, exp, o);
    } catch (RuntimeException e) {
      logger.warn("failed to get async cas future, region {}, primary node {}, {}",
          region, getPrimaryNodeString(keyWithRegion), e.toString());
      return CompletableFuture.completedFuture(CASResponse.ERROR);
    }

    return getCompletableFutureFromOperationFuture(casResponseFuture, region, null)
        .thenApply(HHSpyMemcachedClient::getCASResponseFromSpyCASResponse);
  }

 @Override
  public long increment(String region, String key, int by, int def) {
   String keyWithRegion = getKey(region, key);

   try {
     return spyMemcachedClient.incr(keyWithRegion, by, def);
   } catch (RuntimeException e) {
     logger.warn("failed to increment key value, region {}, primary node {}, {}, returning -1",
         region, getPrimaryNodeString(keyWithRegion), e.toString());
     return -1;
   }
  }

  @Override
  public InetSocketAddress getPrimaryNodeAddress(String key) {
    return (InetSocketAddress) spyMemcachedClient.getConnection().getLocator().getPrimary(key).getSocketAddress();
  }

  private String getPrimaryNodeString(String key) {
    return getPrimaryNodeAddress(key).getHostString();
  }

  @SuppressWarnings(value = "unchecked")
  <T> CompletableFuture<T> getCompletableFutureFromOperationFuture(OperationFuture<T> operationFuture, String region, T fallback) {
    CompletableFuture<T> completableFuture = new CompletableFuture<>();
    operationFuture.addListener(future -> {
      T result;
      try {
        result = (T) future.get();
      } catch (Throwable throwable) {
        logger.warn("got exception while converting spy future to completable future, region {}, primary node {}, {}, returning {}",
            region, getPrimaryNodeString(future.getKey()), throwable.toString(), fallback);
        result = fallback;
      }
      completableFuture.complete(result);
    });
    completableFuture.whenComplete((completableFutureValue, exception) -> {
      if (exception instanceof CancellationException) {
        operationFuture.cancel();
      }
    });
    return completableFuture;
  }

  public static String getKey(String region, String key) {
    return region + key;
  }

  private static CASResponse getCASResponseFromSpyCASResponse(net.spy.memcached.CASResponse casResponse) {
    switch (casResponse) {
      case OK:
        return CASResponse.OK;
      case NOT_FOUND:
        return CASResponse.NOT_FOUND;
      case EXISTS:
        return CASResponse.EXISTS;
      default:
        return CASResponse.ERROR;
    }
  }
}
