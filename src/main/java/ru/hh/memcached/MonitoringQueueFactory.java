package ru.hh.memcached;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationQueueFactory;
import ru.hh.metrics.Max;
import ru.hh.metrics.StatsDSender;
import ru.hh.metrics.Tag;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class MonitoringQueueFactory implements OperationQueueFactory {

  private final int capacity;
  private final String serviceName;
  private final String queueName;
  private final AtomicInteger idGenerator = new AtomicInteger(1);
  private final StatsDSender statsDSender;

  MonitoringQueueFactory(int cap, String serviceName, String queueName, StatsDSender statsDSender) {
    this.capacity = cap;
    this.serviceName = serviceName;
    this.queueName = queueName;
    this.statsDSender = statsDSender;
  }

  @Override
  public BlockingQueue<Operation> create() {
    Max maxSizeCollector = new Max(0);
    BlockingQueue<Operation> queue = new MonitoringArrayBlockingQueue<>(capacity, maxSizeCollector);
    statsDSender.sendMaxPeriodically(
        serviceName + ".memcached.maxQueueSize",
        maxSizeCollector,
        new Tag("queue", queueName),
        new Tag("id", Integer.toString(idGenerator.getAndIncrement()))
    );
    return queue;
  }

  static class MonitoringArrayBlockingQueue<E> extends ArrayBlockingQueue<E> {

    private final Max maxSizeCollector;

    MonitoringArrayBlockingQueue(int capacity, Max maxSizeCollector) {
      super(capacity);
      this.maxSizeCollector = maxSizeCollector;
    }

    // we may not override 'add' because it calls 'offer'

    @Override
    public boolean offer(E e) {
      boolean offered = super.offer(e);
      if (offered) {
        maxSizeCollector.save(size());
      }
      return offered;
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
      boolean offered = super.offer(e, timeout, unit);
      if (offered) {
        maxSizeCollector.save(size());
      }
      return offered;
    }

    @Override
    public void put(E e) throws InterruptedException {
      super.put(e);
      maxSizeCollector.save(size());
    }

    // we may not override 'addAll' because it calls 'add'
  }

}
