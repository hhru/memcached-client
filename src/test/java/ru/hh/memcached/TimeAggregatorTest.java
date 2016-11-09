package ru.hh.memcached;

import com.timgroup.statsd.StatsDClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TimeAggregatorTest {
  private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  private static final StatsDClient statsDClient = mock(StatsDClient.class);

  private static TimeAggregator timeAggregator;

  @Before
  public void setUp() {
    timeAggregator = new TimeAggregator(statsDClient, scheduledExecutorService);
  }

  @After
  public void tearDown() {
    reset(statsDClient);
  }

  @Test
  public void incrementTimeStatsShouldBeSend() throws InterruptedException {
    for (int i = 0; i < 50; i++) {
      timeAggregator.incrementTime("localhost", 100);
    }
    for (int i = 0; i < 45; i++) {
      timeAggregator.incrementTime("localhost", 200);
    }
    for (int i = 0; i < 2; i++) {
      timeAggregator.incrementTime("localhost", 300);
    }
    for (int i = 0; i < 2; i++) {
      timeAggregator.incrementTime("localhost", 400);
    }

    timeAggregator.sendPercentiles();
    verify(statsDClient).gauge("memcached.time.percentile_is_50.targetServer_is_localhost", 100);
    verify(statsDClient).gauge("memcached.time.percentile_is_97.targetServer_is_localhost", 300);
    verify(statsDClient).gauge("memcached.time.percentile_is_99.targetServer_is_localhost", 400);
    verify(statsDClient).gauge("memcached.time.percentile_is_100.targetServer_is_localhost", 400);
    verifyNoMoreInteractions(statsDClient);
    reset(statsDClient);

    for (int i = 0; i < 50; i++) {
      timeAggregator.incrementTime("localhost", 1);
    }
    for (int i = 0; i < 50; i++) {
      timeAggregator.incrementTime("localhost", 2);
    }
    for (int i = 0; i < 23; i++) {
      timeAggregator.incrementTime("localhost", 3);
    }
    for (int i = 0; i < 12; i++) {
      timeAggregator.incrementTime("localhost", 4);
    }

    timeAggregator.sendPercentiles();
    verify(statsDClient).gauge("memcached.time.percentile_is_50.targetServer_is_localhost", 2);
    verify(statsDClient).gauge("memcached.time.percentile_is_97.targetServer_is_localhost", 4);
    verify(statsDClient).gauge("memcached.time.percentile_is_99.targetServer_is_localhost", 4);
    verify(statsDClient).gauge("memcached.time.percentile_is_100.targetServer_is_localhost", 4);
    verifyNoMoreInteractions(statsDClient);
  }

  @Test
  public void sendPercentilesWithoutValues() {
    timeAggregator.sendPercentiles();
    verifyNoMoreInteractions(statsDClient);
  }

  @Test
  public void sendPercentilesOneValue() {
    timeAggregator.incrementTime("localhost", 1);
    timeAggregator.sendPercentiles();

    verify(statsDClient).gauge("memcached.time.percentile_is_50.targetServer_is_localhost", 1);
    verify(statsDClient).gauge("memcached.time.percentile_is_97.targetServer_is_localhost", 1);
    verify(statsDClient).gauge("memcached.time.percentile_is_99.targetServer_is_localhost", 1);
    verify(statsDClient).gauge("memcached.time.percentile_is_100.targetServer_is_localhost", 1);
    verifyNoMoreInteractions(statsDClient);
  }

  @Test
  public void sendPercentilesTwoValues() {
    timeAggregator.incrementTime("localhost", 1);
    timeAggregator.incrementTime("localhost", 2);
    timeAggregator.sendPercentiles();

    verify(statsDClient).gauge("memcached.time.percentile_is_50.targetServer_is_localhost", 1);
    verify(statsDClient).gauge("memcached.time.percentile_is_97.targetServer_is_localhost", 2);
    verify(statsDClient).gauge("memcached.time.percentile_is_99.targetServer_is_localhost", 2);
    verify(statsDClient).gauge("memcached.time.percentile_is_100.targetServer_is_localhost", 2);
    verifyNoMoreInteractions(statsDClient);
  }

  @Test
  public void sendPercentilesTwoTargetServers() {
    timeAggregator.incrementTime("localhost", 1);
    timeAggregator.incrementTime("google", 2);
    timeAggregator.sendPercentiles();

    verify(statsDClient).gauge("memcached.time.percentile_is_50.targetServer_is_localhost", 1);
    verify(statsDClient).gauge("memcached.time.percentile_is_97.targetServer_is_localhost", 1);
    verify(statsDClient).gauge("memcached.time.percentile_is_99.targetServer_is_localhost", 1);
    verify(statsDClient).gauge("memcached.time.percentile_is_100.targetServer_is_localhost", 1);

    verify(statsDClient).gauge("memcached.time.percentile_is_50.targetServer_is_google", 2);
    verify(statsDClient).gauge("memcached.time.percentile_is_97.targetServer_is_google", 2);
    verify(statsDClient).gauge("memcached.time.percentile_is_99.targetServer_is_google", 2);
    verify(statsDClient).gauge("memcached.time.percentile_is_100.targetServer_is_google", 2);

    verifyNoMoreInteractions(statsDClient);
  }
}
