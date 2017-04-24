package ru.hh.memcached;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.BulkFuture;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ru.hh.memcached.HHSpyMemcachedClient.getKey;

public class HHSpyClientGetSomeTest {

  private final MemcachedClient spyClientMock = TestUtils.createSpyClientMock();
  private final HHSpyMemcachedClient hhSpyClient = new HHSpyMemcachedClient(spyClientMock);

  @Test
  public void hitMiss() throws ExecutionException, InterruptedException {
    String[] keys = new String[]{"KeyHit", "KeyMiss", "KeyNull"};
    String[] keysWithRegion = new String[keys.length];
    for (int i=0; i<keys.length; i++) {
      keysWithRegion[i] = getKey("region", keys[i]);
    }

    BulkFuture<Map<String, Object>> bulkFutureMock = mock(BulkFuture.class);
    when(spyClientMock.asyncGetBulk(keysWithRegion)).thenReturn(bulkFutureMock);

    when(spyClientMock.getOperationTimeout()).thenReturn(20L);

    Map<String, Object> bulkFutureResult = new HashMap<>();
    bulkFutureResult.put("regionKeyHit", "value");
    bulkFutureResult.put("regionKeyNull", null);
    when(bulkFutureMock.getSome(20L, TimeUnit.MILLISECONDS)).thenReturn(bulkFutureResult);

    Map<String, Object> keyToValue = hhSpyClient.getSome("region", keys);

    assertEquals(1, keyToValue.size());
    assertEquals("value", keyToValue.get("KeyHit"));
  }
}
