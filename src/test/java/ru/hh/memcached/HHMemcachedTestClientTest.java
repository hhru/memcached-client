package ru.hh.memcached;

import org.junit.After;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HHMemcachedTestClientTest {

  private static final HHMemcachedTestClient hhMemcachedClient = new HHMemcachedTestClient();

  @After
  public void tearDown() {
    hhMemcachedClient.cleanCache();
  }

  @Test
  public void getSome() {
    Map<String, Object> keyToValue = hhMemcachedClient.getSome("region", new String[]{"key1", "key2", "key3"});
    assertTrue(keyToValue.isEmpty());

    hhMemcachedClient.add("region", "key1", 300, "val1");
    hhMemcachedClient.add("region", "key2", 300, "val2");

    keyToValue = hhMemcachedClient.getSome("region", new String[]{"key1", "key2", "key3"});
    assertEquals(2, keyToValue.size());
    assertEquals("val1", keyToValue.get("key1"));
    assertEquals("val2", keyToValue.get("key2"));
    assertNull(keyToValue.get("key3"));

    assertTrue(hhMemcachedClient.getSome("region", new String[]{}).isEmpty());
  }

  @Test
  public void add() throws ExecutionException, InterruptedException {
    CompletableFuture<Boolean> addFuture = hhMemcachedClient.add("region", "key", 300, "val1");
    assertTrue(addFuture.get());
    assertEquals("val1", hhMemcachedClient.get("region", "key"));

    addFuture = hhMemcachedClient.add("region", "key", 300, "val2");
    assertFalse(addFuture.get());
    assertEquals("val1", hhMemcachedClient.get("region", "key"));
  }

  @Test
  public void set() throws ExecutionException, InterruptedException {
    assertNull(hhMemcachedClient.get("region", "key"));

    CompletableFuture<Boolean> setFuture = hhMemcachedClient.set("region", "key", 300, "val1");
    assertTrue(setFuture.get());
    assertEquals("val1", hhMemcachedClient.get("region", "key"));

    setFuture = hhMemcachedClient.set("region", "key", 300, "val2");
    assertTrue(setFuture.get());
    assertEquals("val2", hhMemcachedClient.get("region", "key"));
  }

  @Test
  public void increment() {
    assertEquals(0, hhMemcachedClient.increment("region", "key", 1, 0));
    assertEquals(0, hhMemcachedClient.get("region", "key"));

    assertEquals(1, hhMemcachedClient.increment("region", "key", 1, 0));
    assertEquals(1, hhMemcachedClient.get("region", "key"));

    assertEquals(3, hhMemcachedClient.increment("region", "key", 2, 0));
    assertEquals(3, hhMemcachedClient.get("region", "key"));

    hhMemcachedClient.set("region", "key", 300, "val");
    assertEquals(-1, hhMemcachedClient.increment("region", "key", 1, 0));
    assertNull(hhMemcachedClient.get("region", "key"));
  }

  @Test
  public void incrementWithTtl() {
    assertEquals(0, hhMemcachedClient.increment("region", "key", 1, 0));
    assertEquals(0, hhMemcachedClient.get("region", "key"));

    assertEquals(1, hhMemcachedClient.increment("region", "key", 1, 0));
    assertEquals(1, hhMemcachedClient.get("region", "key"));

    assertEquals(3, hhMemcachedClient.increment("region", "key", 2, 0));
    assertEquals(3, hhMemcachedClient.get("region", "key"));

    hhMemcachedClient.set("region", "key", 300, "val");
    assertEquals(-1, hhMemcachedClient.increment("region", "key", 1, 0));
    assertNull(hhMemcachedClient.get("region", "key"));
  }

  @Test
  public void asyncCASAndGets() throws ExecutionException, InterruptedException {
    CompletableFuture<CASResponse> casFuture = hhMemcachedClient.asyncCas("region", "key", 666L, 300, "val1");
    assertEquals(CASResponse.NOT_FOUND, casFuture.get());
    assertNull(hhMemcachedClient.gets("region", "key"));

    hhMemcachedClient.add("region", "key", 300, "val1");
    CASPair casPair = hhMemcachedClient.gets("region", "key");
    assertEquals("val1", casPair.value);

    casFuture = hhMemcachedClient.asyncCas("region", "key", casPair.casID, 300, "val2");
    assertEquals(CASResponse.OK, casFuture.get());
    assertEquals("val2", hhMemcachedClient.get("region", "key"));

    casFuture = hhMemcachedClient.asyncCas("region", "key", casPair.casID, 300, "val3");
    assertEquals(CASResponse.EXISTS, casFuture.get());
    assertEquals("val2", hhMemcachedClient.get("region", "key"));
  }

  @Test
  public void delete() throws ExecutionException, InterruptedException {
    CompletableFuture<Boolean> deleteFuture = hhMemcachedClient.delete("region", "key");
    assertFalse(deleteFuture.get());

    hhMemcachedClient.add("region", "key", 300, "val");

    deleteFuture = hhMemcachedClient.delete("region", "key");
    assertTrue(deleteFuture.get());
    assertNull(hhMemcachedClient.get("region", "key"));
  }

}
