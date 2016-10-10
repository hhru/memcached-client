package ru.hh.memcached;

import com.timgroup.statsd.StatsDClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class TimeAggregator {
  private static final int PERIOD_OF_TRANSMISSION_STATS_SECONDS = 30;
  private static final int MAX_NUM_OF_TARGET_SERVERS = 20;
  private static final Logger logger = LoggerFactory.getLogger(TimeAggregator.class);

  private final int maxHistogramSize;
  private final StatsDClient statsDClient;
  private final Map<String, Map<Integer, LongAdder>> targetServerToTimeHistogram = new ConcurrentHashMap<>();
  private final List<Double> chances;

  public TimeAggregator(StatsDClient statsDClient, ScheduledExecutorService scheduledExecutorService) {
    this(statsDClient, scheduledExecutorService, 1000, Arrays.asList(0.5, 0.95, 0.97, 0.99));
  }

  public TimeAggregator(StatsDClient statsDClient, ScheduledExecutorService scheduledExecutorService, int maxHistogramSize,
                        List<Double> chances) {
    this.statsDClient = statsDClient;
    this.maxHistogramSize = maxHistogramSize;
    this.chances = chances;

    scheduledExecutorService.scheduleAtFixedRate(this::sendPercentiles, PERIOD_OF_TRANSMISSION_STATS_SECONDS,
        PERIOD_OF_TRANSMISSION_STATS_SECONDS, TimeUnit.SECONDS);
  }

  public void incrementTime(String targetServer, int time) {
    Map<Integer, LongAdder> timeHistogram = targetServerToTimeHistogram.computeIfAbsent(targetServer, key -> {
      if (targetServerToTimeHistogram.size() >= MAX_NUM_OF_TARGET_SERVERS) {
        logger.error("Max number of target memcached servers reached, dropping observation");
        return null;
      } else {
        return new ConcurrentHashMap<>();
      }
    });

    if (timeHistogram != null) {
      LongAdder longAdder = timeHistogram.computeIfAbsent(time, key -> {
        if (timeHistogram.size() >= maxHistogramSize) {
          logger.error("Max number of different duration values reached, dropping observation for server {}", targetServer);
          return null;
        }
        return new LongAdder();
      });
      if (longAdder != null) {
        longAdder.increment();
      }
    }
  }

  void sendPercentiles() {
    for (String targetServer : targetServerToTimeHistogram.keySet()) {
      Map<Integer, LongAdder> timeHistogram = targetServerToTimeHistogram.remove(targetServer);
      int totalObservations = timeHistogram.values().stream()
          .mapToInt(LongAdder::intValue)
          .sum();

      int currentObservations = 0;
      int currentChanceIndex = 0;

      Iterator<Map.Entry<Integer, LongAdder>> it = timeHistogram.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .iterator();
      while (it.hasNext()) {
        Map.Entry<Integer, LongAdder> timeToCountEntry = it.next();

        currentObservations += timeToCountEntry.getValue().intValue();
        for (; currentChanceIndex < chances.size() && totalObservations * chances.get(currentChanceIndex) <= currentObservations;
             currentChanceIndex++) {
          statsDClient.gauge(
              "memcached.time.percentile_is_" + (int) (100 * chances.get(currentChanceIndex))
                  + ".targetServer_is_" + targetServer, timeToCountEntry.getKey());
        }
      }
    }
  }
}
