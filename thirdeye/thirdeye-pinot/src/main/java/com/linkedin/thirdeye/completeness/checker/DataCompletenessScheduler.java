package com.linkedin.thirdeye.completeness.checker;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The scheduler which will run periodically and schedule jobs and tasks for data completeness
 */
public class DataCompletenessScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(DataCompletenessScheduler.class);

  private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  private DataCompletenessJobRunner dataCompletenessJobRunner;
  private DataCompletenessJobContext dataCompletenessJobContext;

  public void start() {
    LOG.info("Starting data completeness checker service");

    dataCompletenessJobContext = new DataCompletenessJobContext();
    dataCompletenessJobRunner = new DataCompletenessJobRunner(dataCompletenessJobContext);

    scheduledExecutorService.scheduleWithFixedDelay(dataCompletenessJobRunner, 0, 15, TimeUnit.MINUTES);
  }

  public void shutdown() {
    scheduledExecutorService.shutdown();
  }

}
