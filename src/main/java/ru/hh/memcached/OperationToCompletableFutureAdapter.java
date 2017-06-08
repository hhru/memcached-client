package ru.hh.memcached;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;

class OperationToCompletableFutureAdapter<T> extends CompletableFuture<T> {
  private final OperationFuture<T> origFuture;

  @SuppressWarnings(value = "unchecked")
  OperationToCompletableFutureAdapter(OperationFuture<T> origFuture) {
    this.origFuture = origFuture;

    OperationCompletionListener operationCompletionListener = future -> {
      try {
        super.complete((T) future.get());
      } catch (Throwable throwable) {
        if (throwable instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        super.completeExceptionally(throwable);
      }
    };
    origFuture.addListener(operationCompletionListener);

    super.whenComplete((completableFutureValue, exception) -> {
      if (exception instanceof CancellationException) {
        origFuture.removeListener(operationCompletionListener);
        origFuture.cancel();
      }
    });
  }

  // Call 'get' of the underlying future directly because:
  // - it has default timeout
  // - timeout doesn't work if get was not called
  // - it tracks timeouts to decide if node is healthy
  @Override
  public T get() throws InterruptedException, ExecutionException {
    return origFuture.get();
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return origFuture.get(timeout, unit);
  }
}
