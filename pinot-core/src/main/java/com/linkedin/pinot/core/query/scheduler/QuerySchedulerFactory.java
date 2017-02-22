/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.query.scheduler;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.core.query.scheduler.tokenbucket.TokenBucketScheduler;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QuerySchedulerFactory {
  private static final String DEFAULT_QUERY_SCHEDULER_ALGORITHM = "fcfs";
  private static Logger LOGGER = LoggerFactory.getLogger(QuerySchedulerFactory.class);

  public static @Nonnull  QueryScheduler create(@Nonnull Configuration schedulerConfig,
      @Nonnull QueryExecutor queryExecutor, ServerMetrics serverMetrics) {
    Preconditions.checkNotNull(schedulerConfig);
    Preconditions.checkNotNull(queryExecutor);

    String algorithm = schedulerConfig.getString("algorithm", DEFAULT_QUERY_SCHEDULER_ALGORITHM).toLowerCase();

    if (algorithm.equals("fcfs")) {
      LOGGER.info("Using FCFS query scheduler");
      return new FCFSQueryScheduler(schedulerConfig, queryExecutor, serverMetrics);
    } else if (algorithm.equals("tokenbucket")) {
      LOGGER.info("Using Priority Token Bucket scheduler");
      return new TokenBucketScheduler(schedulerConfig, queryExecutor, serverMetrics);
    }

    LOGGER.info("Using default FCFS query scheduler");
    return new FCFSQueryScheduler(schedulerConfig, queryExecutor, serverMetrics);
  }
}
