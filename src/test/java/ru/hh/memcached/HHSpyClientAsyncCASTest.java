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

public class HHSpyClientAsyncCASTest {

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
    long casID = 7L;
    int exp = 3;
    Object value = new Object();

    OperationFuture<net.spy.memcached.CASResponse> operationFutureMock = TestUtils.createOperationFutureMock(executorService);
    when(spyClientMock.asyncCAS(keyWithRegion, casID, exp, value)).thenReturn(operationFutureMock);
    doReturn(net.spy.memcached.CASResponse.OK).when(operationFutureMock).get();

    CompletableFuture<CASResponse> casFuture = hhSpyClient.asyncCas("region", "key", casID, 3, value);
    operationFutureMock.signalComplete();

    assertEquals(CASResponse.OK, casFuture.get());
  }

  @Test
  public void problem() throws ExecutionException, InterruptedException {
    String keyWithRegion = HHSpyMemcachedClient.getKey("region", "key");
    long casID = 7L;
    int exp = 3;
    Object value = new Object();

    OperationFuture<net.spy.memcached.CASResponse> operationFutureMock = TestUtils.createOperationFutureMock(executorService);
    when(spyClientMock.asyncCAS(keyWithRegion, casID, exp, value)).thenReturn(operationFutureMock);
    doReturn(net.spy.memcached.CASResponse.OBSERVE_TIMEOUT).when(operationFutureMock).get();

    CompletableFuture<CASResponse> casFuture = hhSpyClient.asyncCas("region", "key", casID, 3, value);
    operationFutureMock.signalComplete();

    assertEquals(CASResponse.ERROR, casFuture.get());
  }
}
