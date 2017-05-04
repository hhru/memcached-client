package ru.hh.memcached;

import java.net.InetSocketAddress;
import java.util.List;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
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

  public static HHMemcachedClient create(Properties properties, StatsDSender statsDSender, String serviceName) throws IOException {
    int opQueueCapacity = parseInt(properties.getProperty("opQueueCapacity"));
    int writeQueueCapacity = parseInt(properties.getProperty("writeOpQueueCapacity"));
    int readQueueCapacity = parseInt(properties.getProperty("readOpQueueCapacity"));
    OperationQueueFactory opQueueFactory;
    OperationQueueFactory writeQueueFactory;
    OperationQueueFactory readQueueFactory;
    if (parseBoolean(properties.getProperty("sendQueuesStats"))) {
      opQueueFactory = new MonitoringQueueFactory(opQueueCapacity, "operation", statsDSender);
      readQueueFactory = new MonitoringQueueFactory(readQueueCapacity, "read", statsDSender);
      writeQueueFactory = new MonitoringQueueFactory(writeQueueCapacity, "write", statsDSender);
    } else {
      opQueueFactory = () -> new ArrayBlockingQueue<>(opQueueCapacity);
      writeQueueFactory = () -> new ArrayBlockingQueue<>(writeQueueCapacity);
      readQueueFactory = () -> new ArrayBlockingQueue<>(readQueueCapacity);
    }

    ConnectionFactory readsConnectionFactory = new ConnectionFactoryBuilder()
        .setOpTimeout(parseInt(properties.getProperty("readOpTimeoutMs")))
        .setOpQueueMaxBlockTime(parseInt(properties.getProperty("opQueueMaxBlockTime")))
        .setOpQueueFactory(opQueueFactory)
        .setWriteOpQueueFactory(writeQueueFactory)
        .setReadOpQueueFactory(readQueueFactory)
        .setFailureMode(FailureMode.valueOf(properties.getProperty("failureMode", FailureMode.Redistribute.name())))
        .setProtocol(Protocol.valueOf(properties.getProperty("protocol", Protocol.BINARY.name())))
        .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT)
        .setHashAlg(DefaultHashAlgorithm.KETAMA_HASH)
        .setMaxReconnectDelay(parseInt(properties.getProperty("maxReconnectDelay")))
        .setTimeoutExceptionThreshold(parseInt(properties.getProperty("timeoutExceptionThreshold")))
        .setDaemon(true)
        .setUseNagleAlgorithm(false)
        .build();
    ConnectionFactory writesConnectionFactory = new ConnectionFactoryBuilder(readsConnectionFactory)
        .setOpTimeout(parseInt(properties.getProperty("writeOpTimeoutMs")))
        .build();

    List<InetSocketAddress> nodes = AddrUtil.getAddresses(properties.getProperty("servers"));

    int numOfInstances = getNumOfInstances(properties);
    int readsToWritesRatio = parseInt(properties.getProperty("readsToWritesRatio", "2"));
    int writesInstances = Math.max(1, numOfInstances / (readsToWritesRatio + 1));
    int readsInstances = Math.max(1, numOfInstances - writesInstances);

    HHMemcachedClient readsMemcachedClient = createHHBalancingMemcachedClient(readsConnectionFactory, nodes, readsInstances);
    HHMemcachedClient writesMemcachedClient = createHHBalancingMemcachedClient(writesConnectionFactory, nodes, writesInstances);
    HHMemcachedClient memcachedClient = new HHReadWriteSplitMemcachedClient(readsMemcachedClient, writesMemcachedClient);

    if (parseBoolean(properties.getProperty("sendStats"))) {
      memcachedClient = new HHMonitoringMemcachedClient(memcachedClient, statsDSender, serviceName);
    }

    return new HHExceptionSwallowerMemcachedClient(memcachedClient);
  }

  private static int getNumOfInstances(Properties properties) {
    String numOfInstancesStr = properties.getProperty("numOfInstances");
    if (numOfInstancesStr == null) {
      // divide availableProcessors / 2 because each memcached client instance is at least 2 threads: MemcachedConnection and transcoder,
      // they create roughly the same CPU load
      return Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
    } else {
      return parseInt(numOfInstancesStr);
    }
  }

  private static HHMemcachedClient createHHBalancingMemcachedClient(ConnectionFactory connectionFactory,
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
