package ru.hh.memcached;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

abstract class HHMemcachedDelegateClient implements HHMemcachedClient {
  private final HHMemcachedClient delegate;

  HHMemcachedDelegateClient(HHMemcachedClient delegate) {
    this.delegate = delegate;
  }

  @Override
  public Object get(String region, String key) {
    return delegate.get(region, key);
  }

  @Override
  public Map<String, Object> getSome(String region, String[] keys) {
    return delegate.getSome(region, keys);
  }

  @Override
  public CompletableFuture<Boolean> set(String region, String key, int exp, Object o) {
    return delegate.set(region, key, exp, o);
  }

  @Override
  public CompletableFuture<Boolean> delete(String region, String key) {
    return delegate.delete(region, key);
  }

  @Override
  public CASPair gets(String region, String key) {
    return delegate.gets(region, key);
  }

  @Override
  public CompletableFuture<Boolean> add(String region, String key, int exp, Object o) {
    return delegate.add(region, key, exp, o);
  }

  @Override
  public CompletableFuture<CASResponse> asyncCas(String region, String key, long casId, int exp, Object o) {
    return delegate.asyncCas(region, key, casId, exp, o);
  }

  @Override
  public long increment(String region, String key, int by, int def) {
    return delegate.increment(region, key, by, def);
  }

  @Override
  public InetSocketAddress getServerAddress(String key) {
    return delegate.getServerAddress(key);
  }
}
