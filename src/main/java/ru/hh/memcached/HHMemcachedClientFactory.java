package ru.hh.memcached;

import com.timgroup.statsd.StatsDClient;
import java.util.concurrent.ScheduledExecutorService;
import net.spy.memcached.AddrUtil;
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
    final MemcachedClient client = new MemcachedClient(builder.build(), AddrUtil.getAddresses(properties.getProperty("memcached.servers")));
    return new HHSpyMemcachedClient(client);
  }

  public static HHMemcachedClient create(Properties properties, StatsDClient client, ScheduledExecutorService scheduledExecutorService) throws IOException {
    return new HHMonitoringMemcachedClient(create(properties), client, scheduledExecutorService);
  }
}
