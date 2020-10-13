package ru.hh.memcached;

import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.BulkFuture;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class HHSpyMemcachedClient implements HHMemcachedClient {
  private final MemcachedClient spyMemcachedClient;

  HHSpyMemcachedClient(MemcachedClient memcachedClient) {
    this.spyMemcachedClient = memcachedClient;
  }

  @Override
  public Object get(String region, String key) {
    String keyWithRegion = getKey(region, key);
    return spyMemcachedClient.get(keyWithRegion);
  }

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

      throw new RuntimeException(e);
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
    return new OperationToCompletableFutureAdapter<>(spyMemcachedClient.set(keyWithRegion, exp, o));
  }

  @Override
  public CompletableFuture<Boolean> delete(String region, String key) {
    String keyWithRegion = getKey(region, key);
    return new OperationToCompletableFutureAdapter<>(spyMemcachedClient.delete(keyWithRegion));
  }

  @Override
  public CASPair gets(String region, String key) {
    String keyWithRegion = getKey(region, key);
    CASValue<Object> casValue = spyMemcachedClient.gets(keyWithRegion);
    return casValue != null ? new CASPair<>(casValue.getCas(), casValue.getValue()) : null;
  }

  @Override
  public CompletableFuture<Boolean> add(String region, String key, int exp, Object o) {
    String keyWithRegion = getKey(region, key);
    return new OperationToCompletableFutureAdapter<>(spyMemcachedClient.add(keyWithRegion, exp, o));
  }

  @Override
  public CompletableFuture<CASResponse> asyncCas(String region, String key, long casId, int exp, Object o) {
    String keyWithRegion = getKey(region, key);
    return new OperationToCompletableFutureAdapter<>(spyMemcachedClient.asyncCAS(keyWithRegion, casId, exp, o))
        .thenApply(HHSpyMemcachedClient::getCASResponseFromSpyCASResponse);
  }

 @Override
  public long increment(String region, String key, int by, int def) {
   String keyWithRegion = getKey(region, key);
   return spyMemcachedClient.incr(keyWithRegion, by, def);
  }

  @Override
  public long increment(String region, String key, int by, int def, int ttl) {
    String keyWithRegion = getKey(region, key);
    return spyMemcachedClient.incr(keyWithRegion, by, def, ttl);
  }

  @Override
  public InetSocketAddress getPrimaryNodeAddress(String key) {
    return (InetSocketAddress) spyMemcachedClient.getConnection().getLocator().getPrimary(key).getSocketAddress();
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
