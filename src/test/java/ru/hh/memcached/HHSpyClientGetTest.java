package ru.hh.memcached;

import net.spy.memcached.MemcachedClient;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class HHSpyClientGetTest {

  private final MemcachedClient spyClientMock = TestUtils.createSpyClientMock();
  private final HHSpyMemcachedClient hhSpyClient = new HHSpyMemcachedClient(spyClientMock);

  @Test
  public void hit() {
    String keyWithRegion = HHSpyMemcachedClient.getKey("region", "key");
    when(spyClientMock.get(keyWithRegion)).thenReturn("value");

    assertEquals("value", hhSpyClient.get("region", "key"));
  }

  @Test
  public void miss() {
    String keyWithRegion = HHSpyMemcachedClient.getKey("region", "key");
    when(spyClientMock.get(keyWithRegion)).thenReturn(null);

    assertNull(hhSpyClient.get("region", "key"));
  }
}
