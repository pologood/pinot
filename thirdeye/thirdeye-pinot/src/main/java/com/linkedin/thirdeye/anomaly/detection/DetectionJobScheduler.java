package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.CronExpression;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;

/**
 * Scheduler for anomaly detection jobs
 */
public class DetectionJobScheduler implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobScheduler.class);
  private ScheduledExecutorService scheduledExecutorService;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY  = ThirdEyeCacheRegistry.getInstance();
  private DetectionJobRunner detectionJobRunner = new DetectionJobRunner();

  private final ConcurrentHashMap<BackfillKey, Thread> existingBackfillJobs = new ConcurrentHashMap<>();

  public DetectionJobScheduler() {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  private final int MAX_BACKFILL_RETRY = 3;
  private final long SYNC_SLEEP_SECONDS = 5;

  public void start() throws SchedulerException {
    scheduledExecutorService.scheduleAtFixedRate(this, 0, 15, TimeUnit.MINUTES);
  }

  public void shutdown() throws SchedulerException{
    scheduledExecutorService.shutdown();
  }

  /**
   * Reads all active anomaly functions
   * For each function, finds all time periods for which detection needs to be run
   * Calls run anomaly function for all those periods, and updates detection status
   * {@inheritDoc}
   * @see java.lang.Runnable#run()
   */
  public void run() {

    // read all anomaly functions
    LOG.info("Reading all anomaly functions");
    List<AnomalyFunctionDTO> anomalyFunctions = DAO_REGISTRY.getAnomalyFunctionDAO().findAllActiveFunctions();

    // for each active anomaly function
    for (AnomalyFunctionDTO anomalyFunction : anomalyFunctions) {

      try {
        LOG.info("Function: {}", anomalyFunction);
        long functionId = anomalyFunction.getId();
        String dataset = anomalyFunction.getCollection();
        DatasetConfigDTO datasetConfig = CACHE_REGISTRY.getDatasetConfigCache().get(dataset);
        DateTimeZone dateTimeZone = Utils.getDataTimeZone(dataset);
        DateTime currentDateTime = new DateTime(dateTimeZone);

        // find last entry into detectionStatus table, for this function
        DetectionStatusDTO lastEntryForFunction = DAO_REGISTRY.getDetectionStatusDAO().
            findLatestEntryForFunctionId(functionId);
        LOG.info("Function: {} Dataset: {} Last entry is {}", functionId, dataset, lastEntryForFunction);

        // calculate entries from last entry to current time
        Map<String, Long> newEntries = DetectionJobSchedulerUtils.getNewEntries(currentDateTime, lastEntryForFunction,
            anomalyFunction, datasetConfig, dateTimeZone);
        LOG.info("Function: {} Dataset: {} Creating {} new entries {}", functionId, dataset, newEntries.size(), newEntries);

        // create these entries
        for (Entry<String, Long> entry : newEntries.entrySet()) {
          DetectionStatusDTO detectionStatus = new DetectionStatusDTO();
          detectionStatus.setDataset(anomalyFunction.getCollection());
          detectionStatus.setFunctionId(functionId);
          detectionStatus.setDateToCheckInSDF(entry.getKey());
          detectionStatus.setDateToCheckInMS(entry.getValue());
          DAO_REGISTRY.getDetectionStatusDAO().save(detectionStatus);
        }

        // find all entries in the past 3 days, which are still isRun = false
        List<DetectionStatusDTO> entriesInLast3Days = DAO_REGISTRY.getDetectionStatusDAO().
            findAllInTimeRangeForFunctionAndDetectionRun(currentDateTime.minusDays(3).getMillis(),
                currentDateTime.getMillis(), functionId, false);
        Collections.sort(entriesInLast3Days);
        LOG.info("Function: {} Dataset: {} Entries in last 3 days {}", functionId, dataset, entriesInLast3Days);

        // for each entry, collect startTime and endTime
        List<Long> startTimes = new ArrayList<>();
        List<Long> endTimes = new ArrayList<>();
        List<DetectionStatusDTO> detectionStatusToUpdate = new ArrayList<>();

        for (DetectionStatusDTO detectionStatus : entriesInLast3Days) {

          try {
            LOG.info("Function: {} Dataset: {} Entry : {}", functionId, dataset, detectionStatus);

            long dateToCheck = detectionStatus.getDateToCheckInMS();
            // check availability for monitoring window - delay
            long endTime = dateToCheck - TimeUnit.MILLISECONDS.convert(anomalyFunction.getWindowDelay(), anomalyFunction.getWindowDelayUnit());
            long startTime = endTime - TimeUnit.MILLISECONDS.convert(anomalyFunction.getWindowSize(), anomalyFunction.getWindowUnit());
            LOG.info("Function: {} Dataset: {} Checking start:{} {} to end:{} {}", functionId, dataset, startTime, new DateTime(startTime, dateTimeZone), endTime, new DateTime(endTime, dateTimeZone));

            boolean pass = checkIfDetectionRunCriteriaMet(startTime, endTime, datasetConfig, anomalyFunction);
            if (pass) {
              startTimes.add(startTime);
              endTimes.add(endTime);
              detectionStatusToUpdate.add(detectionStatus);
            } else {
              LOG.warn("Function: {} Dataset: {} Data incomplete for monitoring window {} ({}) to {} ({}), skipping anomaly detection",
                  functionId, dataset, startTime, new DateTime(startTime), endTime, new DateTime(endTime));
              // TODO: Send email to owners/dev team
            }
          } catch (Exception e) {
            LOG.error("Function: {} Dataset: {} Exception in preparing entry {}", functionId, dataset, detectionStatus, e);
          }
        }

        // If any time periods found, for which detection needs to be run
        runAnomalyFunctionAndUpdateDetectionStatus(startTimes, endTimes, anomalyFunction, detectionStatusToUpdate);

      } catch (Exception e) {
        LOG.error("Function: {} Dataset: {} Exception in running anomaly function {}", anomalyFunction.getId(), anomalyFunction.getCollection(), anomalyFunction, e);
      }
    }
  }

  /**
   * Runs anomaly functions on ranges, and updates detection status
   * @param startTimes
   * @param endTimes
   * @param anomalyFunction
   * @param detectionStatusToUpdate
   */
  private Long runAnomalyFunctionAndUpdateDetectionStatus(List<Long> startTimes, List<Long> endTimes,
      AnomalyFunctionDTO anomalyFunction, List<DetectionStatusDTO> detectionStatusToUpdate) {
    Long jobExecutionId = null;
    if (!startTimes.isEmpty() && !endTimes.isEmpty() && startTimes.size() == endTimes.size()) {
      jobExecutionId = runAnomalyFunctionOnRanges(anomalyFunction, startTimes, endTimes);
      LOG.info("Function: {} Dataset: {} Created job {} for running anomaly function {} on ranges {} to {}",
          anomalyFunction.getId(), anomalyFunction.getCollection(), jobExecutionId, anomalyFunction, startTimes, endTimes);

      for (DetectionStatusDTO detectionStatus : detectionStatusToUpdate) {
        LOG.info("Function: {} Dataset: {} Updating detection run status {} to true",
            anomalyFunction.getId(), anomalyFunction.getCollection(), detectionStatus);
        detectionStatus.setDetectionRun(true);
        DAO_REGISTRY.getDetectionStatusDAO().update(detectionStatus);
      }
    }
    return jobExecutionId;
  }


  /**
   * Point of entry for rest endpoints calling adhoc anomaly functions
   * TODO: Not updating detection status in case of adhoc currently, reconsider
   * @param functionId
   * @param startTime
   * @param endTime
   * @return job execution id
   */
  public Long runAdhocAnomalyFunction(Long functionId, Long startTime, Long endTime) {
    Long jobExecutionId = null;

    AnomalyFunctionDTO anomalyFunction = DAO_REGISTRY.getAnomalyFunctionDAO().findById(functionId);

    String dataset = anomalyFunction.getCollection();
    DatasetConfigDTO datasetConfig = null;
    try {
      datasetConfig = CACHE_REGISTRY.getDatasetConfigCache().get(dataset);
    } catch (ExecutionException e) {
      LOG.error("Function: {} Dataset: {} Exception in fetching dataset config", functionId, dataset, e);
    }

    boolean pass = checkIfDetectionRunCriteriaMet(startTime, endTime, datasetConfig, anomalyFunction);
    if (pass) {
      jobExecutionId = runAnomalyFunctionOnRanges(anomalyFunction, Lists.newArrayList(startTime), Lists.newArrayList(endTime));
    } else {
      LOG.warn("Function: {} Dataset: {} Data incomplete for monitoring window {} ({}) to {} ({}), skipping anomaly detection",
          functionId, dataset, startTime, new DateTime(startTime), endTime, new DateTime(endTime));
      // TODO: Send email to owners/dev team
    }
    return jobExecutionId;
  }

  /**
   * Checks if a time range for a dataset meets data completeness criteria
   * @param startTime
   * @param endTime
   * @param datasetConfig
   * @param anomalyFunction
   * @return true if data completeness check is requested and passes, or data completeness check is not requested at all
   * false if data completeness check is requested and fails
   */
  private boolean checkIfDetectionRunCriteriaMet(Long startTime, Long endTime, DatasetConfigDTO datasetConfig, AnomalyFunctionDTO anomalyFunction) {
    boolean pass = false;
    String dataset = datasetConfig.getDataset();

    /**
     * Check is completeness check required is set at dataset level. That flag is false by default, so user will set as needed
     * Check also for same flag in function level. That flag is true by default, so dataset config's flag will have its way unless user has tampered with this flag
     * This flag would typically be unset, in backfill cases
     */
    if (datasetConfig.isRequiresCompletenessCheck() && anomalyFunction.isRequiresCompletenessCheck()) {

      LOG.info("Function: {} Dataset: {} Checking for completeness of time range {}({}) to {}({})",
          anomalyFunction.getId(), dataset, startTime, new DateTime(startTime), endTime, new DateTime(endTime));

      List<DataCompletenessConfigDTO> incompleteTimePeriods = DAO_REGISTRY.getDataCompletenessConfigDAO().
          findAllByDatasetAndInTimeRangeAndStatus(dataset, startTime, endTime, false);
      LOG.info("Function: {} Dataset: {} Incomplete periods {}", anomalyFunction.getId(), dataset, incompleteTimePeriods);

      if (incompleteTimePeriods.size() == 0) { // nothing incomplete
        // find complete buckets
        List<DataCompletenessConfigDTO> completeTimePeriods = DAO_REGISTRY.getDataCompletenessConfigDAO().
            findAllByDatasetAndInTimeRangeAndStatus(dataset, startTime, endTime, true);
        LOG.info("Function: {} Dataset: {} Complete periods {}", anomalyFunction.getId(), dataset, completeTimePeriods);
        long expectedCompleteBuckets = DetectionJobSchedulerUtils.getExpectedCompleteBuckets(datasetConfig, startTime, endTime);
        LOG.info("Function: {} Dataset: {} Num complete periods: {} Expected num buckets:{}",
            anomalyFunction.getId(), dataset, completeTimePeriods.size(), expectedCompleteBuckets);

        if (completeTimePeriods.size() == expectedCompleteBuckets) { // complete matches expected
          LOG.info("Function: {} Dataset: {}  Found complete time range {}({}) to {}({})",
              anomalyFunction.getId(), dataset, startTime, new DateTime(startTime), endTime, new DateTime(endTime));
          pass = true;
        }
      }
    } else { // no check required
      pass = true;
    }
    return pass;
  }

  /**
   * Creates detection context and runs anomaly job, returns jobExecutionId
   * @param anomalyFunction
   * @param startTimes
   * @param endTimes
   * @return
   */
  private Long runAnomalyFunctionOnRanges(AnomalyFunctionDTO anomalyFunction, List<Long> startTimes, List<Long> endTimes) {
    DetectionJobContext detectionJobContext = new DetectionJobContext();
    detectionJobContext.setAnomalyFunctionId(anomalyFunction.getId());
    detectionJobContext.setAnomalyFunctionSpec(anomalyFunction);
    detectionJobContext.setJobName(DetectionJobSchedulerUtils.createJobName(anomalyFunction, startTimes, endTimes));
    detectionJobContext.setStartTimes(startTimes);
    detectionJobContext.setEndTimes(endTimes);
    Long jobExecutionId = detectionJobRunner.run(detectionJobContext);
    LOG.info("Function: {} Dataset: {} Created job {}", anomalyFunction.getId(), anomalyFunction.getCollection(), jobExecutionId);
    return jobExecutionId;
  }


  /**
   * Sequentially performs anomaly detection for all the monitoring windows that are located between backfillStartTime
   * and backfillEndTime. A lightweight job is performed right after each detection job and notified is set to false in
   * order to silence the mail alerts.
   *
   * NOTE: We assume that the backfill window for the same function DOES NOT overlap. In other words, this function
   * does not guarantees correctness of the detections result if it is invoked twice with the same parameters.
   *
   * @param functionId the id of the anomaly function, which has to be an active function
   * @param backfillStartTime the start time for backfilling
   * @param backfillEndTime the end time for backfilling
   * @param force set to false to resume from previous backfill if there exists any
   * @return task id
   */
  public Long runBackfill(long functionId, DateTime backfillStartTime, DateTime backfillEndTime, boolean force) {
    AnomalyFunctionDTO anomalyFunction = DAO_REGISTRY.getAnomalyFunctionDAO().findById(functionId);
    Long jobId = null;
    String dataset = anomalyFunction.getCollection();
    boolean isActive = anomalyFunction.getIsActive();
    if (!isActive) {
      LOG.info("Skipping function {}", functionId);
      return null;
    }

    BackfillKey backfillKey = new BackfillKey(functionId, backfillStartTime, backfillEndTime);
    Thread returnedThread = existingBackfillJobs.putIfAbsent(backfillKey, Thread.currentThread());
    // If returned thread is not current thread, then a backfill job is already running
    if (returnedThread != null) {
      LOG.info("Function: {} Dataset: {} Aborting... An existing back-fill job is running...", functionId, dataset);
      return null;
    }

    try {
      CronExpression cronExpression = null;
      try {
        cronExpression = new CronExpression(anomalyFunction.getCron());
      } catch (ParseException e) {
        LOG.error("Function: {} Dataset: {} Failed to parse cron expression", functionId, dataset);
        return null;
      }

      long monitoringWindowSize = TimeUnit.MILLISECONDS.convert(anomalyFunction.getWindowSize(), anomalyFunction.getWindowUnit());
      DateTime currentStart;
      if (force) {
        currentStart = backfillStartTime;
      } else {
        currentStart = computeResumeStartTime(functionId, cronExpression, backfillStartTime, backfillEndTime);
      }
      DateTime currentEnd = currentStart.plus(monitoringWindowSize);

      // Make the end time inclusive
      DateTime endBoundary = new DateTime(cronExpression.getNextValidTimeAfter(backfillEndTime.toDate()));

      List<Long> startTimes = new ArrayList<>();
      List<Long> endTimes = new ArrayList<>();

      LOG.info("Function: {} Dataset: {} Begin regenerate anomalies for each monitoring window between {} and {}",
          functionId, dataset, currentStart, endBoundary);
      while (currentEnd.isBefore(endBoundary)) {

        if (Thread.currentThread().isInterrupted()) {
          LOG.info("Function: {} Dataset: {} Terminating adhoc function.", functionId, dataset);
          return null;
        }
        String monitoringWindowStart = ISODateTimeFormat.dateHourMinute().print(currentStart);
        String monitoringWindowEnd = ISODateTimeFormat.dateHourMinute().print(currentEnd);
        LOG.info("Function: {} Dataset: {} Adding adhoc time range {}({}) to {}({})",
            functionId, dataset, currentStart, monitoringWindowStart, currentEnd, monitoringWindowEnd);

        startTimes.add(currentStart.getMillis());
        endTimes.add(currentEnd.getMillis());
        currentStart = new DateTime(cronExpression.getNextValidTimeAfter(currentStart.toDate()));
        currentEnd = currentStart.plus(monitoringWindowSize);
      }

      // If any time periods found, for which detection needs to be run, run anomaly function update detection status
      List<DetectionStatusDTO> findAllInTimeRange = DAO_REGISTRY.getDetectionStatusDAO()
          .findAllInTimeRangeForFunctionAndDetectionRun(backfillStartTime.getMillis(), currentStart.getMillis(), functionId, false);
      jobId = runAnomalyFunctionAndUpdateDetectionStatus(startTimes, endTimes, anomalyFunction, findAllInTimeRange);
      LOG.info("Function: {} Dataset: {} Generated job for detecting anomalies for each monitoring window "
          + "whose start is located in range {} -- {}", functionId, dataset, backfillStartTime, currentStart);
    } finally {
      existingBackfillJobs.remove(backfillKey, Thread.currentThread());
    }
    return jobId;
  }

  /**
   * Different from asynchronous backfill in runBackfill, it will return after the backfill is done.
   * This function monitors the backfill task status, and return once the tasks are completed.
   * @param functionId
   * the function id to be backfilled
   * @param backfillStartTime
   * the monitor start time for backfill
   * @param backfillEndTime
   * the monitor end time for backfill
   * @param force
   * set to false to resume from previous backfill if there exists any
   */
  public void synchronousBackFill(long functionId, DateTime backfillStartTime, DateTime backfillEndTime, boolean force) {
    Long jobExecutionId = runBackfill(functionId, backfillStartTime, backfillEndTime, force);

    if (jobExecutionId == null) {
      LOG.warn("Unable to perform backfill on function Id {} between {} and {}", functionId, backfillStartTime,
          backfillEndTime);
      return;
    }

    TaskManager taskDAO = DAO_REGISTRY.getTaskDAO();
    JobManager jobDAO = DAO_REGISTRY.getJobDAO();
    JobDTO jobDTO = null;
    List<TaskDTO> scheduledTaskDTO = taskDAO.findByJobIdStatusNotIn(jobExecutionId, TaskConstants.TaskStatus.COMPLETED);

    int retryCounter = 0;
    while (scheduledTaskDTO.size() > 0) {
      boolean hasFailedTask = false;
      for (TaskDTO taskDTO : scheduledTaskDTO) {
        if (taskDTO.getStatus() == TaskConstants.TaskStatus.FAILED) {
          if (retryCounter >= MAX_BACKFILL_RETRY) {
            LOG.warn("The backfill of anomaly function {} (task id: {}) failed. Stop retry.", functionId, jobExecutionId);

            // Set job to be failed
            jobDTO = jobDAO.findById(jobExecutionId);
            if (jobDTO.getStatus() != JobConstants.JobStatus.FAILED) {
              jobDTO.setStatus(JobConstants.JobStatus.FAILED);
              jobDAO.save(jobDTO);
            }

            return;
          }
          hasFailedTask = true;
          LOG.warn("The backfill of anomaly function {} (task id: {}) failed. Retry count: {}", functionId,
              jobExecutionId, retryCounter);
        }
      }
      if (hasFailedTask) {
        retryCounter++;
        jobExecutionId = runBackfill(functionId, backfillStartTime, backfillEndTime, force);
      }
      try {
        TimeUnit.SECONDS.sleep(SYNC_SLEEP_SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("The monitoring thread for anomaly function {} (task id: {}) backfill is awakened.", functionId,
            jobExecutionId);
      }
      scheduledTaskDTO = taskDAO.findByJobIdStatusNotIn(jobExecutionId, TaskConstants.TaskStatus.COMPLETED);
    }
    // Set job to be completed
    jobDTO = jobDAO.findById(jobExecutionId);
    if (jobDTO.getStatus() != JobConstants.JobStatus.COMPLETED) {
      jobDTO.setStatus(JobConstants.JobStatus.COMPLETED);
      jobDAO.save(jobDTO);
    }
  }

  private JobDTO getPreviousJob(long functionId, long backfillWindowStart, long backfillWindowEnd) {
    return DAO_REGISTRY.getJobDAO().findLatestBackfillScheduledJobByFunctionId(functionId, backfillWindowStart, backfillWindowEnd);
  }

  /**
   * Returns the start time of the first detection job for the current backfill. The start time is determined in the
   * following:
   * 1. If there exists any previously left detection job, then start backfill from that job.
   *    1a. if that job is finished, then start a job next to it.
   *    1b. if that job is unfinished, then restart that job.
   * 2. If there exists no previous left job, then start the job from the beginning.
   *
   * @param cronExpression the cron expression that is used to calculate the alignment of start time.
   * @return the start time for the first detection job of this backfilling.
   */
  private DateTime computeResumeStartTime(long functionId, CronExpression cronExpression, DateTime backfillStartTime, DateTime backfillEndTime) {
    DateTime currentStart;
    JobDTO previousJob = getPreviousJob(functionId, backfillStartTime.getMillis(), backfillEndTime.getMillis());
    if (previousJob != null) {
      long previousStartTime = previousJob.getWindowStartTime();
      cleanUpJob(previousJob);
      if (previousJob.getStatus().equals(JobConstants.JobStatus.COMPLETED)) {
        // Schedule a job after previous job
        currentStart = new DateTime(cronExpression.getNextValidTimeAfter(new Date(previousStartTime)));
      } else {
        // Reschedule the previous incomplete job
        currentStart = new DateTime(previousStartTime);
      }
      LOG.info("Backfill starting from {} for function {} because a previous unfinished job found.", currentStart,
          functionId);
    } else {
      // Schedule a job starting from the beginning
      currentStart = backfillStartTime;
    }
    return currentStart;
  }

  /**
   * Sets unfinished (i.e., RUNNING, WAITING) tasks and job's status to FAILED
   * @param job
   */
  private void cleanUpJob(JobDTO job) {
    if (!job.getStatus().equals(JobConstants.JobStatus.COMPLETED)) {
      List<TaskDTO> tasks = DAO_REGISTRY.getTaskDAO().findByJobIdStatusNotIn(job.getId(), TaskConstants.TaskStatus.COMPLETED);
      if (CollectionUtils.isNotEmpty(tasks)) {
        for (TaskDTO task : tasks) {
          task.setStatus(TaskConstants.TaskStatus.FAILED);
          DAO_REGISTRY.getTaskDAO().save(task);
        }
        job.setStatus(JobConstants.JobStatus.FAILED);
      } else {
        // This case happens when scheduler dies before it knows that all its tasks are actually finished
        job.setStatus(JobConstants.JobStatus.COMPLETED);
      }
      DAO_REGISTRY.getJobDAO().save(job);
    }
  }


  /**
   * Use to check if the backfill jobs exists
   */
  static class BackfillKey {
    private long functionId;
    private DateTime backfillStartTime;
    private DateTime backfillEndTime;

    public BackfillKey(long functionId, DateTime backfillStartTime, DateTime backfillEndTime){
      this.functionId = functionId;
      this.backfillStartTime = backfillStartTime;
      this.backfillEndTime = backfillEndTime;
    }

    @Override
    public int hashCode() {
      return Objects.hash(functionId, backfillStartTime, backfillEndTime);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof BackfillKey) {
        BackfillKey other = (BackfillKey) o;
        return Objects.equals(this.functionId, other.functionId) && Objects.equals(this.backfillStartTime,
            other.backfillStartTime) && Objects.equals(this.backfillEndTime, other.backfillEndTime);
      } else {
        return false;
      }
    }
  }
}
