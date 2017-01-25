package ru.hh.memcached;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.internal.OperationFuture;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestUtils {

  static MemcachedClient createSpyClientMock() {
    MemcachedNode nodeMock = mock(MemcachedNode.class);
    when(nodeMock.getSocketAddress()).thenReturn(InetSocketAddress.createUnresolved("127.0.0.1", 11211));

    NodeLocator nodeLocatorMock = mock(NodeLocator.class);
    when(nodeLocatorMock.getPrimary(anyString())).thenReturn(nodeMock);

    MemcachedConnection connectionMock = mock(MemcachedConnection.class);
    when(connectionMock.getLocator()).thenReturn(nodeLocatorMock);

    MemcachedClient spyClientMock = mock(MemcachedClient.class);
    when(spyClientMock.getConnection()).thenReturn(connectionMock);

    return spyClientMock;
  }

  static <T> OperationFuture<T> createOperationFutureMock(ExecutorService executorService) {
    OperationFuture<T> operationFuture = new OperationFuture<>("key", new CountDownLatch(0), 1000, executorService);
    operationFuture = spy(operationFuture);
    doReturn(false).when(operationFuture).isDone();
    return operationFuture;
  }

  private TestUtils() {
  }
}
