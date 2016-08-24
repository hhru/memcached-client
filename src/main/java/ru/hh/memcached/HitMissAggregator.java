package ru.hh.memcached;

import com.timgroup.statsd.StatsDClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class HitMissAggregator {
  private static final int MAX_NUM_OF_REGIONS = 300;
  private static final Logger logger = LoggerFactory.getLogger(HitMissAggregator.class);

  private final Map<String, LongAdder> regionToHits = new ConcurrentHashMap<>();
  private final Map<String, LongAdder> regionToMisses = new ConcurrentHashMap<>();

  public HitMissAggregator(final StatsDClient statsDClient) {
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread thread = new Thread(r, "memcached_stats_sender");
      thread.setDaemon(true);
      return thread;
    });
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      regionToHits.forEach((key, value) ->
          statsDClient.gauge("memcached.hitMiss.hitMiss_is_hit.region_is_" + key, value.longValue()));
      regionToMisses.forEach((key, value) ->
          statsDClient.gauge("memcached.hitMiss.hitMiss_is_miss.region_is_" + key, value.longValue()));
    }, 30, 30, TimeUnit.SECONDS);
  }

  public void incrementHit(String region) {
    incrementMetric(region, regionToHits);
  }

  public void incrementMiss(String region) {
    incrementMetric(region, regionToMisses);
  }

  private static void incrementMetric(String region, Map<String, LongAdder> map) {
    map.computeIfAbsent(region.replace('.', '-'), key -> {
      if (map.size() >= MAX_NUM_OF_REGIONS) {
        long minValue = Long.MAX_VALUE;
        String regionToRemove = null;

        for (Map.Entry<String, LongAdder> entry : map.entrySet()) {
          long value = entry.getValue().longValue();
          if (value < minValue) {
            regionToRemove = entry.getKey();
            minValue = value;
          }
        }

        map.remove(regionToRemove);
        logger.error("Max num of regions reached, reset region {} counter", regionToRemove);
      }

      return new LongAdder();
    }).increment();
  }
}
