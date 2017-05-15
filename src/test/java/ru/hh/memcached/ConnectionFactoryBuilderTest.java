package ru.hh.memcached;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
import net.spy.memcached.KetamaNodeLocator;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.ops.OperationQueueFactory;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConnectionFactoryBuilderTest {

  @Test
  public void copy() throws IOException {
    int opTimeout = 25;
    int opQueueMaxBlockTime = 0;
    int maxReconnectDelay = 2;
    int timeoutExceptionThreshold = 5;

    int expectedOpQueueCapacity = 200;
    OperationQueueFactory opQueueFactory = () -> new ArrayBlockingQueue<>(expectedOpQueueCapacity);
    int expectedWriteOpQueueCapacity = 100;
    OperationQueueFactory writeOpQueueFactory = () -> new ArrayBlockingQueue<>(expectedWriteOpQueueCapacity);
    int expectedReadOpQueueCapacity = 50;
    OperationQueueFactory readOpQueueFactory = () -> new ArrayBlockingQueue<>(expectedReadOpQueueCapacity);

    ConnectionFactoryBuilder sourceBuilder = new ConnectionFactoryBuilder()
        .setOpTimeout(opTimeout)
        .setOpQueueMaxBlockTime(opQueueMaxBlockTime)
        .setOpQueueFactory(opQueueFactory)
        .setWriteOpQueueFactory(writeOpQueueFactory)
        .setReadOpQueueFactory(readOpQueueFactory)
        .setFailureMode(FailureMode.Cancel)
        .setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
        .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT)
        .setHashAlg(DefaultHashAlgorithm.KETAMA_HASH)
        .setMaxReconnectDelay(maxReconnectDelay)
        .setTimeoutExceptionThreshold(timeoutExceptionThreshold)
        .setDaemon(true)
        .setUseNagleAlgorithm(false);

    ConnectionFactory factoryFromCopiedBuilder = new ConnectionFactoryBuilder(sourceBuilder).build();

    assertEquals(opTimeout, factoryFromCopiedBuilder.getOperationTimeout());
    assertEquals(opQueueMaxBlockTime, factoryFromCopiedBuilder.getOpQueueMaxBlockTime());
    assertEquals(expectedOpQueueCapacity, factoryFromCopiedBuilder.createOperationQueue().remainingCapacity());
    assertEquals(expectedWriteOpQueueCapacity, factoryFromCopiedBuilder.createWriteOperationQueue().remainingCapacity());
    assertEquals(expectedReadOpQueueCapacity, factoryFromCopiedBuilder.createReadOperationQueue().remainingCapacity());
    assertEquals(FailureMode.Cancel, factoryFromCopiedBuilder.getFailureMode());
    assertTrue(factoryFromCopiedBuilder.getOperationFactory() instanceof BinaryOperationFactory);

    List<InetSocketAddress> memcachedHosts = singletonList(new InetSocketAddress(11211));
    MemcachedClient client = new MemcachedClient(factoryFromCopiedBuilder, memcachedHosts);
    assertTrue(client.getNodeLocator() instanceof KetamaNodeLocator);

    assertEquals(maxReconnectDelay, factoryFromCopiedBuilder.getMaxReconnectDelay());
    assertEquals(timeoutExceptionThreshold, factoryFromCopiedBuilder.getTimeoutExceptionThreshold());
    assertTrue(factoryFromCopiedBuilder.isDaemon());
    assertFalse(factoryFromCopiedBuilder.useNagleAlgorithm());
  }
}
