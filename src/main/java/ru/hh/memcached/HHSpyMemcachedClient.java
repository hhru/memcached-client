package ru.hh.memcached;

import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

class HHSpyMemcachedClient implements HHMemcachedClient {
  private final MemcachedClient spyMemcachedClient;

  HHSpyMemcachedClient(MemcachedClient memcachedClient) {
    this.spyMemcachedClient = memcachedClient;
  }

  @Override
  public Object get(String region, String key) {
    return spyMemcachedClient.get(getKey(region, key));
  }

  @Override
  public Map<String, Object> getBulk(String region, String[] keys) {
    String[] keysWithRegion = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      keysWithRegion[i] = getKey(region, keys[i]);
    }

    Map<String, Object> regionKeyToValueMap = spyMemcachedClient.getBulk(keysWithRegion);
    Map<String, Object> keyToValueMap = new HashMap<>();

    for (int i = 0; i < keys.length; i++) {
      Object value = regionKeyToValueMap.get(keysWithRegion[i]);
      if(value != null) {
        keyToValueMap.put(keys[i], value);
      }
    }
    return keyToValueMap;
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
    return (InetSocketAddress) spyMemcachedClient.getNodeLocator().getPrimary(key).getSocketAddress();
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
