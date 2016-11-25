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

class HHSpyMemcachedClient implements HHMemcachedClient {

  private static final Logger logger = LoggerFactory.getLogger(HHSpyMemcachedClient.class);

  private final MemcachedClient spyMemcachedClient;

  HHSpyMemcachedClient(MemcachedClient memcachedClient) {
    this.spyMemcachedClient = memcachedClient;
  }

  @Override
  public Object get(String region, String key) {
    return spyMemcachedClient.get(getKey(region, key));
  }

  /** @return Map of key to value.<br/>
   * The map does not contain keys that are not in memcached or if an error occurs. **/
  @Override
  public Map<String, Object> getSome(String region, String[] keys) {
    String[] keysWithRegion = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      keysWithRegion[i] = getKey(region, keys[i]);
    }

    BulkFuture<Map<String, Object>> bulkFuture = spyMemcachedClient.asyncGetBulk(keysWithRegion);

    Map<String, Object> keyWithRegionToValue;
    try {
      keyWithRegionToValue = bulkFuture.getSome(spyMemcachedClient.getOperationTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      logger.warn("Failed to wait cache, region {}, keys {}, {}, returning empty map", region, keys, e.toString());
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
    return getCompletableFutureFromOperationFuture(spyMemcachedClient.set(getKey(region, key), exp, o));
  }

  @Override
  public CompletableFuture<Boolean> delete(String region, String key) {
    return getCompletableFutureFromOperationFuture(spyMemcachedClient.delete(getKey(region, key)));
  }

  @Override
  public CASPair gets(String region, String key) {
    CASValue<Object> casValue = spyMemcachedClient.gets(getKey(region, key));
    if (casValue == null) {
      return null;
    }
    return new CASPair<>(casValue.getCas(), casValue.getValue());
  }

  @Override
  public CompletableFuture<Boolean> add(String region, String key, int exp, Object o) {
    return getCompletableFutureFromOperationFuture(spyMemcachedClient.add(getKey(region, key), exp, o));
  }

  @Override
  public CompletableFuture<CASResponse> asyncCas(String region, String key, long casId, int exp, Object o) {
    final OperationFuture<net.spy.memcached.CASResponse> operationFuture = spyMemcachedClient.asyncCAS(getKey(region, key), casId, exp, o);
    CompletableFuture<CASResponse> completableFuture = new CompletableFuture<>();
    operationFuture.addListener(future -> {
      try {
        completableFuture.complete(getCASResponseFromSpyMemcachedCASResponse((net.spy.memcached.CASResponse) future.get()));
      } catch (Throwable throwable) {
        completableFuture.completeExceptionally(throwable);
      }
    });
    completableFuture.whenComplete((completableFutureValue, exception) -> {
      if (exception instanceof CancellationException) {
        operationFuture.cancel();
      }
    });
    return completableFuture;
  }

 @Override
  public long increment(String region, String key, int by, int def) {
    return spyMemcachedClient.incr(getKey(region, key), by, def);
  }

  @Override
  public InetSocketAddress getServerAddress(String key) {
    return (InetSocketAddress) spyMemcachedClient.getConnection().getLocator().getPrimary(key).getSocketAddress();
  }

  @SuppressWarnings(value = "unchecked")
  static <T> CompletableFuture<T> getCompletableFutureFromOperationFuture(final OperationFuture<T> operationFuture) {
    CompletableFuture<T> completableFuture = new CompletableFuture<>();
    operationFuture.addListener(future -> {
      try {
        completableFuture.complete((T) future.get());
      } catch (Throwable throwable) {
        completableFuture.completeExceptionally(throwable);
      }
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

  private static CASResponse getCASResponseFromSpyMemcachedCASResponse(net.spy.memcached.CASResponse casResponse) {
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
