package ru.hh.memcached;

import java.net.InetSocketAddress;
import java.util.List;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.ops.OperationQueueFactory;
import ru.hh.metrics.StatsDSender;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;

public class HHMemcachedClientFactory {

  public static HHMemcachedClient create(Properties properties, StatsDSender statsDSender) throws IOException {
    int opQueueCapacity = parseInt(properties.getProperty("memcached.opQueueCapacity"));
    int writeQueueCapacity = parseInt(properties.getProperty("memcached.writeOpQueueCapacity"));
    int readQueueCapacity = parseInt(properties.getProperty("memcached.readOpQueueCapacity"));
    OperationQueueFactory opQueueFactory;
    OperationQueueFactory writeQueueFactory;
    OperationQueueFactory readQueueFactory;
    if (parseBoolean(properties.getProperty("memcached.sendQueuesStats"))) {
      opQueueFactory = new MonitoringQueueFactory(opQueueCapacity, "operation", statsDSender);
      readQueueFactory = new MonitoringQueueFactory(readQueueCapacity, "read", statsDSender);
      writeQueueFactory = new MonitoringQueueFactory(writeQueueCapacity, "write", statsDSender);
    } else {
      opQueueFactory = () -> new ArrayBlockingQueue<>(opQueueCapacity);
      writeQueueFactory = () -> new ArrayBlockingQueue<>(writeQueueCapacity);
      readQueueFactory = () -> new ArrayBlockingQueue<>(readQueueCapacity);
    }

    final ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder()
        .setProtocol(ConnectionFactoryBuilder.Protocol.TEXT)
        .setOpTimeout(parseInt(properties.getProperty("memcached.opTimeoutMs")))
        .setOpQueueMaxBlockTime(parseInt(properties.getProperty("memcached.opQueueMaxBlockTime")))
        .setOpQueueFactory(opQueueFactory)
        .setWriteOpQueueFactory(writeQueueFactory)
        .setReadOpQueueFactory(readQueueFactory)
        .setFailureMode(FailureMode.valueOf(properties.getProperty("memcached.failureMode", FailureMode.Redistribute.name())))
        .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT)
        .setHashAlg(DefaultHashAlgorithm.KETAMA_HASH)
        .setMaxReconnectDelay(parseInt(properties.getProperty("memcached.maxReconnectDelay")))
        .setTimeoutExceptionThreshold(parseInt(properties.getProperty("memcached.timeoutExceptionThreshold")))
        .setDaemon(true)
        .setUseNagleAlgorithm(false);
    ConnectionFactory connectionFactory = builder.build();

    List<InetSocketAddress> nodes = AddrUtil.getAddresses(properties.getProperty("memcached.servers"));

    int numOfInstances = getNumOfInstances(properties);

    HHMemcachedClient memcachedClient = createHHSpyMemcachedClient(connectionFactory, nodes, numOfInstances);
    if (parseBoolean(properties.getProperty("memcached.sendStats"))) {
      return new HHMonitoringMemcachedClient(memcachedClient, statsDSender);
    } else {
      return memcachedClient;
    }
  }

  private static int getNumOfInstances(Properties properties) {
    String numOfInstancesStr = properties.getProperty("memcached.numOfInstances");
    if (numOfInstancesStr == null) {
      // divide availableProcessors / 2 because each memcached client instance is at least 2 threads: MemcachedConnection and transcoder,
      // they create roughly the same CPU load
      return Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
    } else {
      return parseInt(numOfInstancesStr);
    }
  }

  private static HHMemcachedClient createHHSpyMemcachedClient(ConnectionFactory connectionFactory,
                                                              List<InetSocketAddress> nodes,
                                                              int numOfInstances) throws IOException {
    if (numOfInstances == 1) {
      return createHHSpyMemcachedClient(connectionFactory, nodes);
    } else {
      HHMemcachedClient[] clients = new HHMemcachedClient[numOfInstances];
      for (int i=0; i<numOfInstances; i++) {
        clients[i] = createHHSpyMemcachedClient(connectionFactory, nodes);
      }
      return new HHBalancingMemcachedClient(clients);
    }
  }

  private static HHMemcachedClient createHHSpyMemcachedClient(ConnectionFactory connectionFactory,
                                                              List<InetSocketAddress> nodes) throws IOException {
    MemcachedClient client = new MemcachedClient(connectionFactory, nodes);
    return new HHSpyMemcachedClient(client);
  }

}
