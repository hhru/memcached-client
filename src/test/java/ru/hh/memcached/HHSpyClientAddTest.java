package ru.hh.memcached;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

public class HHSpyClientAddTest {

  private final MemcachedClient spyClientMock = TestUtils.createSpyClientMock();
  private final HHSpyMemcachedClient hhSpyClient = new HHSpyMemcachedClient(spyClientMock);
  private static final ExecutorService executorService = Executors.newSingleThreadExecutor();

  @AfterClass
  public static void afterHHSpyClientSetTestClass() {
    executorService.shutdown();
  }

  @Test
  public void success() throws ExecutionException, InterruptedException {
    String keyWithRegion = HHSpyMemcachedClient.getKey("region", "key");
    int exp = 3;
    Object value = new Object();

    OperationFuture<Boolean> operationFutureMock = TestUtils.createOperationFutureMock(executorService);
    when(spyClientMock.add(keyWithRegion, exp, value)).thenReturn(operationFutureMock);
    doReturn(true).when(operationFutureMock).get();

    CompletableFuture<Boolean> addFuture = hhSpyClient.add("region", "key", exp, value);
    operationFutureMock.signalComplete();

    assertEquals(true, addFuture.get());
  }

  @Test
  public void problem() throws ExecutionException, InterruptedException {
    String keyWithRegion = HHSpyMemcachedClient.getKey("region", "key");
    int exp = 3;
    Object value = new Object();

    OperationFuture<Boolean> operationFutureMock = TestUtils.createOperationFutureMock(executorService);
    when(spyClientMock.add(keyWithRegion, exp, value)).thenReturn(operationFutureMock);
    doReturn(false).when(operationFutureMock).get();

    CompletableFuture<Boolean> addFuture = hhSpyClient.add("region", "key", exp, value);
    operationFutureMock.signalComplete();

    assertEquals(false, addFuture.get());
  }
}
