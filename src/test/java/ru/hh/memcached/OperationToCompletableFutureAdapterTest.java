package ru.hh.memcached;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import net.spy.memcached.internal.OperationFuture;
import org.junit.AfterClass;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

public class OperationToCompletableFutureAdapterTest {

  private static final ExecutorService executorService = Executors.newSingleThreadExecutor();

  @AfterClass
  public static void afterConvertFutureTestClass() {
    executorService.shutdown();
  }

  @Test
  public void success() throws Exception {
    OperationFuture<Boolean> operationFutureMock = TestUtils.createOperationFutureMock(executorService);
    doReturn(true).when(operationFutureMock).get();

    CompletableFuture<Boolean> completableFuture = new OperationToCompletableFutureAdapter<>(operationFutureMock);
    operationFutureMock.signalComplete();

    assertTrue(completableFuture.get());
  }

  @Test
  public void cancelCompletableFuture() throws Exception {
    OperationFuture<Boolean> operationFutureMock = TestUtils.createOperationFutureMock(executorService);

    CompletableFuture<Boolean> completableFuture = new OperationToCompletableFutureAdapter<>(operationFutureMock);
    completableFuture.cancel(false);

    verify(operationFutureMock).cancel();
  }
}
