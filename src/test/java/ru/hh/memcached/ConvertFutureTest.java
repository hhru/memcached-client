package ru.hh.memcached;

import net.spy.memcached.internal.OperationFuture;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class ConvertFutureTest {
  private static ExecutorService executorService;

  @BeforeClass
  public static void setUp() {
    executorService = Executors.newSingleThreadExecutor();
  }

  @AfterClass
  public static void tearDown() {
    executorService.shutdown();
  }

  @Test
  public void getCompletableFutureFromOperationFutureTest() throws Exception {
    OperationFuture<Boolean> operationFuture = createOperationFuturePartialMock();

    doReturn(false).when(operationFuture).get();

    CompletableFuture<Boolean> completableFuture = HHSpyMemcachedClient.getCompletableFutureFromOperationFuture(operationFuture);

    operationFuture.signalComplete();
    assertFalse(completableFuture.get());
  }

  @Test
  public void getCompletableFutureWhenCanceledOperationFutureCanceled() throws Exception {
    OperationFuture<Boolean> operationFuture = createOperationFuturePartialMock();

    CompletableFuture<Boolean> completableFuture = HHSpyMemcachedClient.getCompletableFutureFromOperationFuture(operationFuture);

    completableFuture.cancel(false);
    verify(operationFuture).cancel();
  }

  @Test(expected = ExecutionException.class)
  public void getCompletableFutureWhenException() throws Exception {
    OperationFuture<Boolean> operationFuture = createOperationFuturePartialMock();

    doThrow(Exception.class).when(operationFuture).get();

    CompletableFuture<Boolean> completableFuture = HHSpyMemcachedClient.getCompletableFutureFromOperationFuture(operationFuture);

    operationFuture.signalComplete();
    completableFuture.get();
  }

  private OperationFuture<Boolean> createOperationFuturePartialMock() {
    OperationFuture<Boolean> operationFuture = new OperationFuture<>("key", new CountDownLatch(0), 1000, executorService);
    operationFuture = spy(operationFuture);
    doReturn(false).when(operationFuture).isDone();
    return operationFuture;
  }
}
