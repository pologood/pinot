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

package com.linkedin.pinot.core.query.scheduler.tokenbucket;

import com.linkedin.pinot.core.query.scheduler.SchedulerQueryContext;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class TableTokenAccount {
  private static Logger LOGGER = LoggerFactory.getLogger(TableTokenAccount.class);

  private final String tableName;
  private final int tokenLifetimeMs;
  private int availableTokens;
  private long lastUpdateTimeMs;
  private final int numTokensPerInterval;
  private int threadsInUse;
  private final List<SchedulerQueryContext> pendingQueries = new ArrayList<>();


  // We replenish tokens at regular intervals instead of doing it continuously (say, every ms).
  // This is to avoid some table to hoard tokens and then block out others for large periods of time
  //
  private static final int TOKEN_REPLENISH_INTERVAL_MS = 100;

  TableTokenAccount(String tableName, int numTokensPerInterval, int tokenLifetimeMs) {
    this.tableName = tableName;
    this.numTokensPerInterval = numTokensPerInterval;
    this.tokenLifetimeMs = tokenLifetimeMs;
    lastUpdateTimeMs = System.currentTimeMillis();
    availableTokens = numTokensPerInterval;
  }

  int getAvailableTokens() {
    consumeTokens();
    return availableTokens;
  }

  void markQueryStart(int numThreads) {
    consumeTokens();
    threadsInUse += numThreads;
  }

  void markQueryEnd(int numThreads) {
    consumeTokens();
    threadsInUse -= numThreads;
  }

  public List<SchedulerQueryContext> getPendingQueries() {
    return pendingQueries;
  }

  // callers must synchronize access to this method
  private void consumeTokens() {
    long currentTimeMs = System.currentTimeMillis();
    // multiple time qantas may have elapsed..hence, the modulo operation
    long diffMs = (currentTimeMs - lastUpdateTimeMs);
    if (diffMs >= tokenLifetimeMs) {
      availableTokens = tokenLifetimeMs * (numTokensPerInterval - threadsInUse);
    } else {
      availableTokens -= diffMs * threadsInUse;
    }
  }

}
