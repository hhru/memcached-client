package ru.hh.memcached;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface HHMemcachedClient {
  Object get(String region, String key);
  Map<String, Object> getBulk(String region, String[] keys);
  CompletableFuture<Boolean> set(String region, String key, int exp, Object o);
  CompletableFuture<Boolean> delete(String region, String key);
  CASPair gets(String region, String key);
  CompletableFuture<Boolean> add(String region, String key, int exp, Object o);
  CompletableFuture<CASResponse> asyncCas(String region, String key, long casId, int exp, Object o);
  long increment(String region, String key, int by, int def);
  InetSocketAddress getServerAddress(String key);
}
