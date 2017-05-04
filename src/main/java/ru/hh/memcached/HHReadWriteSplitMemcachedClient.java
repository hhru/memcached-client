package ru.hh.memcached;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

class HHReadWriteSplitMemcachedClient implements HHMemcachedClient {

  private final HHMemcachedClient readsMemcachedClient;
  private final HHMemcachedClient writesMemcachedClient;

  HHReadWriteSplitMemcachedClient(HHMemcachedClient readsMemcachedClient, HHMemcachedClient writesMemcachedClient) {
    this.readsMemcachedClient = readsMemcachedClient;
    this.writesMemcachedClient = writesMemcachedClient;
  }

  @Nullable
  @Override
  public Object get(String region, String key) {
    return readsMemcachedClient.get(region, key);
  }

  @Override
  public Map<String, Object> getSome(String region, String[] keys) {
    return readsMemcachedClient.getSome(region, keys);
  }

  @Override
  public CompletableFuture<Boolean> set(String region, String key, int exp, Object o) {
    return writesMemcachedClient.set(region, key, exp, o);
  }

  @Override
  public CompletableFuture<Boolean> delete(String region, String key) {
    return writesMemcachedClient.delete(region, key);
  }

  @Nullable
  @Override
  public CASPair gets(String region, String key) {
    return readsMemcachedClient.gets(region, key);
  }

  @Override
  public CompletableFuture<Boolean> add(String region, String key, int exp, Object o) {
    return writesMemcachedClient.add(region, key, exp, o);
  }

  @Override
  public CompletableFuture<CASResponse> asyncCas(String region, String key, long casId, int exp, Object o) {
    return writesMemcachedClient.asyncCas(region, key, casId, exp, o);
  }

  @Override
  public long increment(String region, String key, int by, int def) {
    return writesMemcachedClient.increment(region, key, by, def);
  }

  @Override
  public InetSocketAddress getPrimaryNodeAddress(String key) {
    return readsMemcachedClient.getPrimaryNodeAddress(key);
  }
}
