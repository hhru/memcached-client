package ru.hh.memcached;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

class HHBalancingMemcachedClient implements HHMemcachedClient {

  private final HHMemcachedClient[] clients;

  HHBalancingMemcachedClient(HHMemcachedClient[] clients) {
    this.clients = clients;
  }

  @Nullable
  @Override
  public Object get(String region, String key) {
    return getClient().get(region, key);
  }

  @Override
  public Map<String, Object> getSome(String region, String[] keys) {
    return getClient().getSome(region, keys);
  }

  @Override
  public CompletableFuture<Boolean> set(String region, String key, int exp, Object o) {
    return getClient().set(region, key, exp, o);
  }

  @Override
  public CompletableFuture<Boolean> delete(String region, String key) {
    return getClient().delete(region, key);
  }

  @Nullable
  @Override
  public CASPair gets(String region, String key) {
    return getClient().gets(region, key);
  }

  @Override
  public CompletableFuture<Boolean> add(String region, String key, int exp, Object o) {
    return getClient().add(region, key, exp, o);
  }

  @Override
  public CompletableFuture<CASResponse> asyncCas(String region, String key, long casId, int exp, Object o) {
    return getClient().asyncCas(region, key, casId, exp, o);
  }

  @Override
  public long increment(String region, String key, int by, int def) {
    return getClient().increment(region, key, by, def);
  }

  @Override
  public InetSocketAddress getPrimaryNodeAddress(String key) {
    return getClient().getPrimaryNodeAddress(key);
  }

  private HHMemcachedClient getClient() {
    int delegateIndex = ThreadLocalRandom.current().nextInt(clients.length);
    return clients[delegateIndex];
  }
}
