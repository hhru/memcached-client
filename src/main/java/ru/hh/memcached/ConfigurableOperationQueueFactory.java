package ru.hh.memcached;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationQueueFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class ConfigurableOperationQueueFactory implements OperationQueueFactory {
  private final int capacity;

  ConfigurableOperationQueueFactory(int cap) {
    this.capacity = cap;
  }

  @Override
  public BlockingQueue<Operation> create() {
    if (capacity > 0) {
      return new ArrayBlockingQueue<>(capacity);
    } else {
      return new LinkedBlockingQueue<>();
    }
  }
}
