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
import ru.hh.nab.metrics.StatsDSender;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.util.Optional.ofNullable;

public class HHMemcachedClientFactory {
  private static final int DEFAULT_METRICS_SEND_INTERVAL_SEC = 60;
  private static final int MAX_NUM_OF_HIT_MISS_COUNTERS = 500;
  private static final int MAX_TIME_HISTOGRAM_SIZE = 1000;
  private static final int MAX_NUM_OF_TIME_HISTOGRAMS = 20;
  private static final int MAX_NUM_OF_ERROR_COUNTERS = 500;

  private HHMemcachedClientFactory() {}

  public static HHMemcachedClient create(Properties properties, String serviceName, StatsDSender statsDSender) throws IOException {
    int metricsSendIntervalSec = ofNullable(properties.getProperty("metricsSendIntervalSec")).map(Integer::parseInt)
      .orElse(DEFAULT_METRICS_SEND_INTERVAL_SEC);
    int maxNumOfHitMissCounters = ofNullable(properties.getProperty("maxNumOfHitMissCounters")).map(Integer::parseInt)
      .orElse(MAX_NUM_OF_HIT_MISS_COUNTERS);
    int maxTimeHistogramSize = ofNullable(properties.getProperty("maxTimeHistogramSize")).map(Integer::parseInt)
      .orElse(MAX_TIME_HISTOGRAM_SIZE);
    int maxNumOfTimeHistograms = ofNullable(properties.getProperty("maxNumOfTimeHistograms")).map(Integer::parseInt)
      .orElse(MAX_NUM_OF_TIME_HISTOGRAMS);
    int maxNumOfErrorCounters = ofNullable(properties.getProperty("maxNumOfErrorCounters")).map(Integer::parseInt)
      .orElse(MAX_NUM_OF_ERROR_COUNTERS);

    int opQueueCapacity = parseInt(properties.getProperty("opQueueCapacity"));
    int writeQueueCapacity = parseInt(properties.getProperty("writeOpQueueCapacity"));
    int readQueueCapacity = parseInt(properties.getProperty("readOpQueueCapacity"));
    OperationQueueFactory opQueueFactory;
    OperationQueueFactory writeQueueFactory;
    OperationQueueFactory readQueueFactory;
    if (parseBoolean(properties.getProperty("sendQueuesStats"))) {
      opQueueFactory = new MonitoringQueueFactory(opQueueCapacity, serviceName, "operation", statsDSender, metricsSendIntervalSec);
      readQueueFactory = new MonitoringQueueFactory(readQueueCapacity, serviceName, "read", statsDSender, metricsSendIntervalSec);
      writeQueueFactory = new MonitoringQueueFactory(writeQueueCapacity, serviceName, "write", statsDSender, metricsSendIntervalSec);
    } else {
      opQueueFactory = () -> new ArrayBlockingQueue<>(opQueueCapacity);
      writeQueueFactory = () -> new ArrayBlockingQueue<>(writeQueueCapacity);
      readQueueFactory = () -> new ArrayBlockingQueue<>(readQueueCapacity);
    }

    final ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder()
        .setOpTimeout(parseInt(properties.getProperty("opTimeoutMs")))
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
        .setUseNagleAlgorithm(false);
    ConnectionFactory connectionFactory = builder.build();

    List<InetSocketAddress> nodes = AddrUtil.getAddresses(properties.getProperty("servers"));

    int numOfInstances = getNumOfInstances(properties);

    HHMemcachedClient memcachedClient = createHHSpyMemcachedClient(connectionFactory, nodes, numOfInstances);
    if (parseBoolean(properties.getProperty("sendStats"))) {
      memcachedClient = new HHMonitoringMemcachedClient(memcachedClient, serviceName, statsDSender,
                                                        metricsSendIntervalSec,
                                                        maxNumOfHitMissCounters,
                                                        maxTimeHistogramSize, maxNumOfTimeHistograms,
                                                        maxNumOfErrorCounters);
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
