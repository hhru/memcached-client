package ru.hh.statsd;

import com.timgroup.statsd.StatsDClientErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;

public class StatsDErrorHandler implements StatsDClientErrorHandler {
  private static final Logger logger = LoggerFactory.getLogger(StatsDErrorHandler.class);

  @Override
  public void handle(Exception exception) {
    if (exception instanceof RejectedExecutionException) {
      logger.warn("The StatsD client queue is full, dropping message to statsd");
    } else {
      logger.error("StatsD client error {}", exception.toString(), exception);
    }
  }
}
