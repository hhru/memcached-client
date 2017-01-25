package ru.hh.memcached;

import net.spy.memcached.MemcachedClient;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class HHSpyClientGetsTest {

  private final MemcachedClient spyClientMock = TestUtils.createSpyClientMock();
  private final HHSpyMemcachedClient hhSpyClient = new HHSpyMemcachedClient(spyClientMock);

  @Test
  public void hit() {
    String keyWithRegion = HHSpyMemcachedClient.getKey("region", "key");
    long casID = 123L;
    Object value = new Object();
    when(spyClientMock.gets(keyWithRegion)).thenReturn(new net.spy.memcached.CASValue<>(casID, value));

    CASPair casPair = hhSpyClient.gets("region", "key");

    assertEquals(casID, casPair.casID);
    assertEquals(value, casPair.value);
  }

  @Test
  public void miss() {
    String keyWithRegion = HHSpyMemcachedClient.getKey("region", "key");
    when(spyClientMock.gets(keyWithRegion)).thenReturn(null);

    assertNull(hhSpyClient.gets("region", "key"));
  }

  @Test
  public void exception() {
    String keyWithRegion = HHSpyMemcachedClient.getKey("region", "key");
    when(spyClientMock.gets(keyWithRegion)).thenThrow(RuntimeException.class);

    assertNull(hhSpyClient.gets("region", "key"));
  }

}
