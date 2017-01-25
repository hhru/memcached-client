package ru.hh.memcached;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface HHMemcachedClient {
  @Nullable
  Object get(String region, String key);

  Map<String, Object> getSome(String region, String[] keys);

  CompletableFuture<Boolean> set(String region, String key, int exp, Object o);

  CompletableFuture<Boolean> delete(String region, String key);

  @Nullable
  CASPair gets(String region, String key);

  CompletableFuture<Boolean> add(String region, String key, int exp, Object o);

  CompletableFuture<CASResponse> asyncCas(String region, String key, long casId, int exp, Object o);

  long increment(String region, String key, int by, int def);

  InetSocketAddress getPrimaryNodeAddress(String key);
}
