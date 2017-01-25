package ru.hh.memcached;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static ru.hh.memcached.HHSpyMemcachedClient.getKey;


public class HHMemcachedTestClient implements HHMemcachedClient {
  private static final Map<String, Object> store = new ConcurrentHashMap<>();

  @Override
  public Object get(String region, String key) {
    return store.get(getKey(region, key));
  }

  @Override
  public Map<String, Object> getSome(String region, String[] keys) {
    Map<String, Object> objectMap = new HashMap<>();
    for (String key : keys) {
      Object object = get(region, key);
      if(null != object) {
        objectMap.put(key, object);
      }
    }
    return objectMap;
  }

  @Override
  public CompletableFuture<Boolean> set(String region, String key, int exp, Object newValue) {
    store.put(getKey(region, key), newValue);
    return CompletableFuture.completedFuture(true);
  }

  @Override
  public CompletableFuture<Boolean> delete(String region, String key) {
    store.remove(getKey(region, key));
    return CompletableFuture.completedFuture(true);
  }

  @Override
  public CASPair gets(String region, String key) {
    Object object = get(region, key);
    return new CASPair<>(object.hashCode(), object);
  }

  @Override
  public CompletableFuture<Boolean> add(String region, String key, int exp, Object newValue) {
    if (null != store.putIfAbsent(getKey(region, key), newValue)) {
      return CompletableFuture.completedFuture(false);
    } else {
      return CompletableFuture.completedFuture(true);
    }
  }

  @Override
  public CompletableFuture<CASResponse> asyncCas(String region, String key, long casId, int exp, Object newValue) {
    Object oldValue = get(region, key);
    if (oldValue == null) {
      return CompletableFuture.completedFuture(CASResponse.NOT_FOUND);
    }
    if (oldValue.hashCode() != casId) {
      return CompletableFuture.completedFuture(CASResponse.EXISTS);
    }
    if (store.replace(getKey(region, key), oldValue, newValue)) {
      return CompletableFuture.completedFuture(CASResponse.OK);
    } else {
      return CompletableFuture.completedFuture(CASResponse.EXISTS);
    }
  }

  @Override
  public long increment(String region, String key, int by, int def) {
    Object oldValue = get(region, key);
    int newValue = def;

    if (null != oldValue) {
      try {
        newValue = Integer.parseInt(oldValue.toString());
      } catch (NumberFormatException e) {
        return -1;
      }
      newValue += by;
    }

    if (store.replace(getKey(region, key), oldValue, newValue)) {
      return newValue;
    } else {
      return -1;
    }
  }

  @Override
  public InetSocketAddress getPrimaryNodeAddress(String key) {
    return new InetSocketAddress("127.0.0.1", 11211);
  }

  public void cleanCache() {
    store.clear();
  }
}
