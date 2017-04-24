package ru.hh.memcached;

import net.spy.memcached.MemcachedClient;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class HHSpyClientIncrementTest {

  private final MemcachedClient spyClientMock = TestUtils.createSpyClientMock();
  private final HHSpyMemcachedClient hhSpyClient = new HHSpyMemcachedClient(spyClientMock);

  @Test
  public void success() {
    String keyWithRegion = HHSpyMemcachedClient.getKey("region", "key");
    int by = 1;
    int defaultValue = 1;
    long newValue = 1L;
    when(spyClientMock.incr(keyWithRegion, by, defaultValue)).thenReturn(newValue);

    assertEquals(newValue, hhSpyClient.increment("region", "key", by, defaultValue));
  }
}
