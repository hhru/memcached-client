package ru.hh.memcached;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HHExceptionSwallowerMemcachedClientTest {
  private static final String REGION = "region";
  private static final String KEY = "key";
  private static final ExecutorService executorService = Executors.newSingleThreadExecutor();

  private final HHMemcachedClient hhSpyClient = mock(HHMemcachedClient.class);
  private final HHMemcachedClient hhExceptionSwallowerMemcachedClient = new HHExceptionSwallowerMemcachedClient(hhSpyClient);

  @Before
  public void setUp() {
    when(hhSpyClient.getPrimaryNodeAddress(anyString()))
            .thenReturn(InetSocketAddress.createUnresolved("127.0.0.1", 11211));
  }

  @AfterClass
  public static void afterHHSpyClientSetTestClass() {
    executorService.shutdown();
  }

  @Test
  public void getShouldSwallowException() {
    when(hhSpyClient.get(REGION, KEY)).thenThrow(RuntimeException.class);
    assertNull(hhExceptionSwallowerMemcachedClient.get(REGION, KEY));

    verify(hhSpyClient).get(REGION, KEY);
  }

  @Test
  public void getSomeShouldSwallowException() {
    String[] keys = {KEY};
    when(hhSpyClient.getSome(REGION, keys)).thenThrow(RuntimeException.class);

    assertTrue(hhExceptionSwallowerMemcachedClient.getSome(REGION, keys).isEmpty());
    verify(hhSpyClient).getSome(REGION, keys);
  }

  @Test
  public void setShouldSwallowException() throws Exception {
    int exp = 3;
    Object value = new Object();
    when(hhSpyClient.set(REGION, KEY, exp, value)).thenThrow(RuntimeException.class);

    CompletableFuture<Boolean> setFuture = hhExceptionSwallowerMemcachedClient.set(REGION, KEY, exp, value);

    assertFalse(setFuture.get());
    verify(hhSpyClient).set(REGION, KEY, exp, value);
  }

  @Test
  public void deleteShouldSwallowException() throws Exception {
    when(hhSpyClient.delete(REGION, KEY)).thenThrow(RuntimeException.class);

    CompletableFuture<Boolean> setFuture = hhExceptionSwallowerMemcachedClient.delete(REGION, KEY);

    assertFalse(setFuture.get());
    verify(hhSpyClient).delete(REGION, KEY);
  }

  @Test
  public void getsShouldSwallowException() {
    when(hhSpyClient.gets(REGION, KEY)).thenThrow(RuntimeException.class);

    CASPair casPair = hhExceptionSwallowerMemcachedClient.gets(REGION, KEY);

    assertNull(casPair);
    verify(hhSpyClient).gets(REGION, KEY);
  }

  @Test
  public void addShouldSwallowException() throws Exception {
    int exp = 3;
    Object value = new Object();
    when(hhSpyClient.add(REGION, KEY, exp, value)).thenThrow(RuntimeException.class);

    CompletableFuture<Boolean> addFuture = hhExceptionSwallowerMemcachedClient.add(REGION, KEY, exp, value);

    assertFalse(addFuture.get());
    verify(hhSpyClient).add(REGION, KEY, exp, value);
  }

  @Test
  public void asyncCasShouldSwallowException() throws Exception {
    long casID = 7L;
    int exp = 3;
    Object value = new Object();
    when(hhSpyClient.asyncCas(REGION, KEY, casID, exp, value)).thenThrow(RuntimeException.class);

    CompletableFuture<CASResponse> casFuture = hhExceptionSwallowerMemcachedClient.asyncCas(REGION, KEY, casID, exp, value);

    assertEquals(CASResponse.ERROR, casFuture.get());
    verify(hhSpyClient).asyncCas(REGION, KEY, casID, exp, value);
  }

  @Test
  public void incrementShouldSwallowException() {
    int by = 1;
    int defaultValue = 1;
    when(hhSpyClient.increment(REGION, KEY, by, defaultValue)).thenThrow(RuntimeException.class);

    assertEquals(-1L, hhExceptionSwallowerMemcachedClient.increment(REGION, KEY, by, defaultValue));
    verify(hhSpyClient).increment(REGION, KEY, by, defaultValue);
  }
}
