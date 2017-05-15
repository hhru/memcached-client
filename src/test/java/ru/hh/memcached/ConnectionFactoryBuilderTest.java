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
import static org.junit.Assert.assertNotEquals;
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

    ConnectionFactory connectionFactory = new ConnectionFactoryBuilder()
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
        .setUseNagleAlgorithm(false)
        .build();

    ConnectionFactory buggedCopy = new ConnectionFactoryBuilder(connectionFactory).build();
    ConnectionFactory fixedCopy = HHMemcachedClientFactory.connectionFactoryBuilder(connectionFactory,
        opQueueFactory, writeOpQueueFactory, readOpQueueFactory,
        ConnectionFactoryBuilder.Locator.CONSISTENT, DefaultHashAlgorithm.KETAMA_HASH, ConnectionFactoryBuilder.Protocol.BINARY
    ).build();

    assertEquals(opTimeout, buggedCopy.getOperationTimeout());
    assertEquals(opQueueMaxBlockTime, buggedCopy.getOpQueueMaxBlockTime());

    assertNotEquals("opQueueFactory should not be copied because of the bug in the ConnectionFactoryBuilder constructor, " +
            "remove the assertion and the corresponding fix if the bug is fixed",
        expectedOpQueueCapacity, buggedCopy.createOperationQueue().remainingCapacity());
    assertEquals(expectedOpQueueCapacity, fixedCopy.createOperationQueue().remainingCapacity());

    assertNotEquals("writeOpQueueFactory should not be copied because of the bug in the ConnectionFactoryBuilder constructor, " +
            "remove the assertion and the corresponding fix if the bug is fixed",
        expectedWriteOpQueueCapacity, buggedCopy.createWriteOperationQueue().remainingCapacity());
    assertEquals(expectedWriteOpQueueCapacity, fixedCopy.createWriteOperationQueue().remainingCapacity());

    assertNotEquals("readOpQueueFactory should not be copied because of the bug in the ConnectionFactoryBuilder constructor, " +
        "remove the assertion and the corresponding fix if the bug is fixed",
        expectedReadOpQueueCapacity, buggedCopy.createReadOperationQueue().remainingCapacity());
    assertEquals(expectedReadOpQueueCapacity, fixedCopy.createReadOperationQueue().remainingCapacity());

    assertEquals(FailureMode.Cancel, buggedCopy.getFailureMode());

    assertFalse("protocol should not be copied because of the bug in the ConnectionFactoryBuilder constructor, " +
        "remove the assertion and the corresponding fix if the bug is fixed",
        buggedCopy.getOperationFactory() instanceof BinaryOperationFactory);
    assertTrue(fixedCopy.getOperationFactory() instanceof BinaryOperationFactory);

    List<InetSocketAddress> memcachedHosts = singletonList(new InetSocketAddress(11211));

    MemcachedClient memcachedClientFromBuggedCopy = new MemcachedClient(buggedCopy, memcachedHosts);
    assertFalse("locatorType should not be copied because of the bug in the ConnectionFactoryBuilder constructor, " +
        "remove the assertion and the corresponding fix if the bug is fixed",
        memcachedClientFromBuggedCopy.getNodeLocator() instanceof KetamaNodeLocator);

    MemcachedClient memcachedClientFromFixedCopy = new MemcachedClient(fixedCopy, memcachedHosts);
    assertTrue(memcachedClientFromFixedCopy.getNodeLocator() instanceof KetamaNodeLocator);

    assertEquals(maxReconnectDelay, buggedCopy.getMaxReconnectDelay());

    assertNotEquals("timeoutExceptionThreshold should not be copied because of the bug in the ConnectionFactoryBuilder constructor, " +
        "remove the assertion and the corresponding fix if the bug is fixed",
        timeoutExceptionThreshold, buggedCopy.getTimeoutExceptionThreshold());
    // -2 because of the strange code in ConnectionFactoryBuilder.setTimeoutExceptionThreshold which sets target value to param - 2.
    // Remove -2 if this strange code is fixed.
    assertEquals(timeoutExceptionThreshold - 2, fixedCopy.getTimeoutExceptionThreshold());

    assertTrue(buggedCopy.isDaemon());
    assertFalse(buggedCopy.useNagleAlgorithm());
  }
}
