package ru.hh.memcached;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class HHReadWriteSplitMemcachedClientTest {

  private static final String REGION = "region";
  private static final String KEY = "key";

  private final HHMemcachedClient readsClientMock = mock(HHMemcachedClient.class);
  private final HHMemcachedClient writesClientMock = mock(HHMemcachedClient.class);
  private final HHMemcachedClient splitClient = new HHReadWriteSplitMemcachedClient(readsClientMock, writesClientMock);

  @Test
  public void get() {
    when(readsClientMock.get(REGION, KEY)).thenReturn("value");

    Object result = splitClient.get(REGION, KEY);

    verify(readsClientMock).get(REGION, KEY);
    verifyZeroInteractions(writesClientMock);
    assertEquals("value", result);
  }

  @Test
  public void getSome() {
    String[] keys = {KEY};
    Map<String, Object> expectedKeyToValue = new HashMap<>();
    when(readsClientMock.getSome(REGION, keys)).thenReturn(expectedKeyToValue);

    Map<String, Object> actualKeyToValue = splitClient.getSome(REGION, keys);

    verify(readsClientMock).getSome(REGION, keys);
    verifyZeroInteractions(writesClientMock);
    assertEquals(expectedKeyToValue, actualKeyToValue);
  }

  @Test
  public void gets() {
    long casID = 666L;
    CASPair<String> expectedCAS = new CASPair<>(casID, "value");
    when(readsClientMock.gets(REGION, KEY)).thenReturn(expectedCAS);

    CASPair actualCAS = splitClient.gets(REGION, KEY);

    verify(readsClientMock).gets(REGION, KEY);
    verifyZeroInteractions(writesClientMock);
    assertEquals(expectedCAS, actualCAS);
  }

  @Test
  public void set() {
    int exp = 666;
    CompletableFuture<Boolean> expectedFuture = CompletableFuture.completedFuture(true);
    when(writesClientMock.set(REGION, KEY, exp, "value")).thenReturn(expectedFuture);

    CompletableFuture<Boolean> actualFuture = splitClient.set(REGION, KEY, exp, "value");

    verify(writesClientMock).set(REGION, KEY, exp, "value");
    verifyZeroInteractions(readsClientMock);
    assertEquals(expectedFuture, actualFuture);
  }

  @Test
  public void delete() {
    CompletableFuture<Boolean> expectedFuture = CompletableFuture.completedFuture(true);
    when(writesClientMock.delete(REGION, KEY)).thenReturn(expectedFuture);

    CompletableFuture<Boolean> actualFuture = splitClient.delete(REGION, KEY);

    verify(writesClientMock).delete(REGION, KEY);
    verifyZeroInteractions(readsClientMock);
    assertEquals(expectedFuture, actualFuture);
  }

  @Test
  public void add() {
    int exp = 666;
    CompletableFuture<Boolean> expectedFuture = CompletableFuture.completedFuture(true);
    when(writesClientMock.add(REGION, KEY, exp, "value")).thenReturn(expectedFuture);

    CompletableFuture<Boolean> actualFuture = splitClient.add(REGION, KEY, exp, "value");

    verify(writesClientMock).add(REGION, KEY, exp, "value");
    verifyZeroInteractions(readsClientMock);
    assertEquals(expectedFuture, actualFuture);
  }

  @Test
  public void asyncCas() {
    long casId = 666L;
    int exp = 777;
    CompletableFuture<CASResponse> expectedFuture = CompletableFuture.completedFuture(CASResponse.OK);
    when(writesClientMock.asyncCas(REGION, KEY, casId, exp, "value")).thenReturn(expectedFuture);

    CompletableFuture<CASResponse> actualFuture = splitClient.asyncCas(REGION, KEY, casId, exp, "value");

    verify(writesClientMock).asyncCas(REGION, KEY, casId, exp, "value");
    verifyZeroInteractions(readsClientMock);
    assertEquals(expectedFuture, actualFuture);
  }

  @Test
  public void increment() {
    int by = 2;
    int def = 0;
    long expectedValue = 4L;
    when(writesClientMock.increment(REGION, KEY, by, def)).thenReturn(expectedValue);

    long increment = splitClient.increment(REGION, KEY, by, def);

    verify(writesClientMock).increment(REGION, KEY, by, def);
    verifyZeroInteractions(readsClientMock);
    assertEquals(expectedValue, increment);
  }

}
