package ru.hh.memcached;

import com.timgroup.statsd.StatsDClient;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.util.Properties;

public class HHMemcachedClientFactory {
  public static HHMemcachedClient create(Properties properties) throws IOException {
    final ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder()
        .setProtocol(ConnectionFactoryBuilder.Protocol.TEXT)
        .setOpTimeout(Integer.parseInt(properties.getProperty("memcached.opTimeoutMs")))
        .setOpQueueMaxBlockTime(Integer.parseInt(properties.getProperty("memcached.opQueueMaxBlockTime")))
        .setOpQueueFactory(new ConfigurableOperationQueueFactory(Integer.parseInt(properties.getProperty("memcached.opQueueCapacity"))))
        .setReadOpQueueFactory(new ConfigurableOperationQueueFactory(Integer.parseInt(properties.getProperty("memcached.readOpQueueCapacity"))))
        .setWriteOpQueueFactory(
            new ConfigurableOperationQueueFactory(Integer.parseInt(properties.getProperty("memcached.writeOpQueueCapacity"))))
        .setFailureMode(FailureMode.valueOf(properties.getProperty("memcached.failureMode", FailureMode.Redistribute.name())))
        .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT)
        .setHashAlg(DefaultHashAlgorithm.KETAMA_HASH)
        .setMaxReconnectDelay(Integer.parseInt(properties.getProperty("memcached.maxReconnectDelay")))
        .setTimeoutExceptionThreshold(Integer.parseInt(properties.getProperty("memcached.timeoutExceptionThreshold")))
        .setUseNagleAlgorithm(false);
    ConnectionFactory connectionFactory = builder.build();

    List<InetSocketAddress> nodes = AddrUtil.getAddresses(properties.getProperty("memcached.servers"));

    int numOfInstances = getNumOfInstances(properties);

    return createHHSpyMemcachedClient(connectionFactory, nodes, numOfInstances);
  }

  public static HHMemcachedClient create(Properties properties, StatsDClient client, ScheduledExecutorService scheduledExecutorService) throws IOException {
    return new HHMonitoringMemcachedClient(create(properties), client, scheduledExecutorService);
  }

  private static int getNumOfInstances(Properties properties) {
    String numOfInstancesStr = properties.getProperty("memcached.numOfInstances");
    if (numOfInstancesStr == null) {
      return Runtime.getRuntime().availableProcessors();
    } else {
      return Integer.parseInt(numOfInstancesStr);
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
