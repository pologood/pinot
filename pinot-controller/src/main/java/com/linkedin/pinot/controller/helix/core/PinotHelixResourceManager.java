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
package com.linkedin.pinot.controller.helix.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.OfflineTableConfig;
import com.linkedin.pinot.common.config.RealtimeTableConfig;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableCustomConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.config.TenantConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.messages.SegmentRefreshMessage;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.StateModel.BrokerOnlineOfflineStateModel;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.SchemaUtils;
import com.linkedin.pinot.common.utils.ServerType;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.common.utils.ZkUtils;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.helix.PinotHelixPropertyStoreZnRecordProvider;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.common.utils.retry.RetryPolicy;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.pojos.Instance;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse.ResponseStatus;
import com.linkedin.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import com.linkedin.pinot.controller.helix.core.sharding.SegmentAssignmentStrategy;
import com.linkedin.pinot.controller.helix.core.sharding.SegmentAssignmentStrategyFactory;
import com.linkedin.pinot.controller.helix.core.util.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.core.util.ZKMetadataUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class PinotHelixResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotHelixResourceManager.class);
  private static final long DEFAULT_EXTERNAL_VIEW_UPDATE_TIMEOUT_MILLIS = 120_000L; // 2 minutes
  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 2.0f);

  private String _zkBaseUrl;
  private String _helixClusterName;
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private String _helixZkURL;
  private String _instanceId;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private String _localDiskDir;
  private SegmentDeletionManager _segmentDeletionManager = null;
  private long _externalViewOnlineToOfflineTimeoutMillis = DEFAULT_EXTERNAL_VIEW_UPDATE_TIMEOUT_MILLIS;
  private long _externalViewUpdateRetryInterval = 500L;
  private boolean _isSingleTenantCluster = true;
  private boolean _isUpdateStateModel = false;

  private HelixDataAccessor _helixDataAccessor;
  Builder _keyBuilder;

  private static final Map<String, SegmentAssignmentStrategy> SEGMENT_ASSIGNMENT_STRATEGY_MAP =
      new HashMap<String, SegmentAssignmentStrategy>();

  @SuppressWarnings("unused")
  private PinotHelixResourceManager() {

  }

  public String getHelixZkURL() {
    return _helixZkURL;
  }


  public PinotHelixResourceManager(String zkURL, String helixClusterName, String controllerInstanceId,
      String localDiskDir, long externalViewOnlineToOfflineTimeoutMillis, boolean isSingleTenantCluster,
      boolean isUpdateStateModel) {
    _zkBaseUrl = zkURL;
    _helixClusterName = helixClusterName;
    _instanceId = controllerInstanceId;
    _localDiskDir = localDiskDir;
    _externalViewOnlineToOfflineTimeoutMillis = externalViewOnlineToOfflineTimeoutMillis;
    _isSingleTenantCluster = isSingleTenantCluster;
    _isUpdateStateModel = isUpdateStateModel;
  }

  public PinotHelixResourceManager(String zkURL, String helixClusterName, String controllerInstanceId,
      String localDiskDir) {
    this(zkURL, helixClusterName, controllerInstanceId, localDiskDir, DEFAULT_EXTERNAL_VIEW_UPDATE_TIMEOUT_MILLIS, false, false);
  }

  public PinotHelixResourceManager(ControllerConf controllerConf) {
    this(controllerConf.getZkStr(), controllerConf.getHelixClusterName(),
        controllerConf.getControllerHost() + "_" + controllerConf.getControllerPort(), controllerConf.getDataDir(),
        controllerConf.getExternalViewOnlineToOfflineTimeout(), controllerConf.tenantIsolationEnabled(),
        controllerConf.isUpdateSegmentStateModel());
  }

  public synchronized void start() throws Exception {
    _helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_zkBaseUrl);
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, _helixZkURL, _instanceId, _isUpdateStateModel);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    _propertyStore = ZkUtils.getZkPropertyStore(_helixZkManager, _helixClusterName);
    _helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    _keyBuilder = _helixDataAccessor.keyBuilder();
    _segmentDeletionManager = new SegmentDeletionManager(_localDiskDir, _helixAdmin, _helixClusterName, _propertyStore);
    ZKMetadataProvider.setClusterTenantIsolationEnabled(_propertyStore, _isSingleTenantCluster);
  }

  public synchronized void stop() {
    _segmentDeletionManager.stop();
    _helixZkManager.disconnect();
  }

  public InstanceConfig getHelixInstanceConfig(String instanceId) {
    return _helixAdmin.getInstanceConfig(_helixClusterName, instanceId);
  }

  public List<String> getBrokerInstancesFor(String tableName) {
    String brokerTenantName = null;
    try {
      if (getAllTableNames().contains(TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName))) {
        brokerTenantName =
            ZKMetadataProvider.getRealtimeTableConfig(getPropertyStore(), tableName).getTenantConfig().getBroker();
      }
      if (getAllTableNames().contains(TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName))) {
        brokerTenantName =
            ZKMetadataProvider.getOfflineTableConfig(getPropertyStore(), tableName).getTenantConfig().getBroker();
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception", e);
      Utils.rethrowException(e);
    }
    final List<String> instanceIds = new ArrayList<String>();
    instanceIds.addAll(getAllInstancesForBrokerTenant(brokerTenantName));
    return instanceIds;
  }

  public InstanceZKMetadata getInstanceZKMetadata(String instanceId) {
    return ZKMetadataProvider.getInstanceZKMetadata(getPropertyStore(), instanceId);
  }

  public List<String> getAllRealtimeTables() {
    List<String> ret = _helixAdmin.getResourcesInCluster(_helixClusterName);
    CollectionUtils.filter(ret, new Predicate() {
      @Override
      public boolean evaluate(Object object) {
        if (object == null) {
          return false;
        }
        if (object.toString().endsWith("_" + ServerType.REALTIME.toString())) {
          return true;
        }
        return false;
      }
    });
    return ret;
  }

  public List<String> getAllTableNames() {
    return _helixAdmin.getResourcesInCluster(_helixClusterName);
  }

  public List<String> getAllInstanceNames() {
    return _helixAdmin.getInstancesInCluster(_helixClusterName);
  }

  /**
   * Returns all tables, remove brokerResource.
   */
  public Set<String> getAllUniquePinotRawTableNames() {
    List<String> tableNames = getAllTableNames();

    // Filter table names that are known to be non Pinot tables (ie. brokerResource)
    Set<String> pinotTableNames = new HashSet<>();
    for (String tableName : tableNames) {
      if (CommonConstants.Helix.NON_PINOT_RESOURCE_RESOURCE_NAMES.contains(tableName)) {
        continue;
      }
      pinotTableNames.add(TableNameBuilder.extractRawTableName(tableName));
    }

    return pinotTableNames;
  }

  public List<String> getAllPinotTableNames() {
    List<String> tableNames = getAllTableNames();

    // Filter table names that are known to be non Pinot tables (ie. brokerResource)
    List<String> pinotTableNames = new ArrayList<>();
    for (String tableName : tableNames) {
      if (CommonConstants.Helix.NON_PINOT_RESOURCE_RESOURCE_NAMES.contains(tableName)) {
        continue;
      }
      pinotTableNames.add(tableName);
    }

    return pinotTableNames;
  }
  public synchronized void restartTable(String tableName) {
    final Set<String> allInstances =
        HelixHelper.getAllInstancesForResource(HelixHelper.getTableIdealState(_helixZkManager, tableName));
    HelixHelper.setStateForInstanceSet(allInstances, _helixClusterName, _helixAdmin, false);
    HelixHelper.setStateForInstanceSet(allInstances, _helixClusterName, _helixAdmin, true);
  }

  private boolean ifExternalViewChangeReflectedForState(String tableName, String segmentName, String targetState,
      long timeoutMillis, boolean considerErrorStateAsDifferentFromTarget) {
    long externalViewChangeCompletedDeadline = System.currentTimeMillis() + timeoutMillis;

    deadlineLoop:
    while (System.currentTimeMillis() < externalViewChangeCompletedDeadline) {
      ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, tableName);
      Map<String, String> segmentStatsMap = externalView.getStateMap(segmentName);
      if (segmentStatsMap != null) {
        LOGGER.info("Found {} instances for segment '{}' in external view", segmentStatsMap.size(), segmentName);
        for (String instance : segmentStatsMap.keySet()) {
          final String segmentState = segmentStatsMap.get(instance);

          // jfim: Ignore segments in error state as part of checking if the external view change is reflected
          if (!segmentState.equalsIgnoreCase(targetState)) {
            if ("ERROR".equalsIgnoreCase(segmentState) && !considerErrorStateAsDifferentFromTarget) {
              // Segment is in error and we don't consider error state as different from target, therefore continue
            } else {
              // Will try to read data every 500 ms, only if external view not updated.
              Uninterruptibles.sleepUninterruptibly(_externalViewUpdateRetryInterval, TimeUnit.MILLISECONDS);
              continue deadlineLoop;
            }
          }
        }

        // All segments match with the expected external view state
        return true;
      } else {
        // Segment doesn't exist in EV, wait for a little bit
        Uninterruptibles.sleepUninterruptibly(_externalViewUpdateRetryInterval, TimeUnit.MILLISECONDS);
      }
    }

    // Timed out
    LOGGER.info("Timed out while waiting for segment '{}' to become '{}' in external view.", segmentName, targetState);
    return false;
  }

  private boolean ifSegmentExisted(SegmentMetadata segmentMetadata) {
    if (segmentMetadata == null) {
      return false;
    }
    return _propertyStore.exists(
        ZKMetadataProvider.constructPropertyStorePathForSegment(
            TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(segmentMetadata.getTableName()),
            segmentMetadata.getName()), AccessOption.PERSISTENT);
  }

  private boolean ifRefreshAnExistedSegment(SegmentMetadata segmentMetadata, String segmentName, String tableName) {
    OfflineSegmentZKMetadata offlineSegmentZKMetadata =
        ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, segmentMetadata.getTableName(),
            segmentMetadata.getName());
    if (offlineSegmentZKMetadata == null) {
      LOGGER.info("Rejecting because Zk metadata is null for segment {} of table {}", segmentName, tableName);
      return false;
    }
    final SegmentMetadata existedSegmentMetadata = new SegmentMetadataImpl(offlineSegmentZKMetadata);
    if (segmentMetadata.getIndexCreationTime() <= existedSegmentMetadata.getIndexCreationTime()) {
      LOGGER.info("Rejecting because of older or same creation time {} (we have {}) for segment {} of table {}",
          segmentMetadata.getIndexCreationTime(), existedSegmentMetadata.getIndexCreationTime(), segmentName, tableName);
      return false;
    }
    if (segmentMetadata.getCrc().equals(existedSegmentMetadata.getCrc())) {
      LOGGER.info("Rejecting because of matching CRC exists (incoming={}, existing={}) for {} of table {}",
          segmentMetadata.getCrc(), existedSegmentMetadata.getCrc(), segmentName, tableName);
      return false;
    }
    return true;
  }

  public synchronized PinotResourceManagerResponse addInstance(Instance instance) {
    final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();
    final List<String> instances = HelixHelper.getAllInstances(_helixAdmin, _helixClusterName);
    if (instances.contains(instance.toInstanceId())) {
      resp.status = ResponseStatus.failure;
      resp.message = "Instance " + instance.toInstanceId() + " already exists.";
      return resp;
    } else {
      _helixAdmin.addInstance(_helixClusterName, instance.toInstanceConfig());
      resp.status = ResponseStatus.success;
      resp.message = "";
      return resp;
    }
  }

  /**
   * Deletes segment from helix idealstate and drop it from local storage
   * @param tableName Segment's table name
   * @param segmentName segment name
   * @return
   */
  public synchronized PinotResourceManagerResponse deleteSegment(final String tableName, final String segmentName) {
    return deleteSegments(tableName, Arrays.asList(segmentName));
  }

  /**
   * Deletes a list of segments from idealstate and drop those from the local storage
   * @param tableName Name of segment's table
   * @param segments List of segment names
   * @return
   */
  public synchronized PinotResourceManagerResponse deleteSegments(final String tableName, final List<String> segments) {
    LOGGER.info("Trying to delete segments: {} for table: {} ", StringUtils.join(segments, ','), tableName);
    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    try {
      HelixHelper.removeSegmentsFromIdealState(_helixZkManager, tableName, segments);
      _segmentDeletionManager.deleteSegments(tableName, segments);

      res.message += "Segment " + StringUtils.join(segments, ',') + " successfully deleted.";
      res.status = ResponseStatus.success;
    } catch (final Exception e) {
      LOGGER.error("Caught exception while deleting segment {} of table {}", StringUtils.join(segments, ','), tableName, e);
      res.status = ResponseStatus.failure;
      res.message = e.getMessage();
    }
    return res;
  }

  // **** End Broker level operations ****
  public void startInstances(List<String> instances) {
    HelixHelper.setStateForInstanceList(instances, _helixClusterName, _helixAdmin, false);
    HelixHelper.setStateForInstanceList(instances, _helixClusterName, _helixAdmin, true);
  }

  @Override
  public String toString() {
    return "yay! i am alive and kicking, clusterName is : " + _helixClusterName + " zk url is : " + _zkBaseUrl;
  }

  public ZkHelixPropertyStore<ZNRecord> getPropertyStore() {
    return _propertyStore;
  }

  public HelixAdmin getHelixAdmin() {
    return _helixAdmin;
  }

  public String getHelixClusterName() {
    return _helixClusterName;
  }

  public boolean isLeader() {
    return _helixZkManager.isLeader();
  }

  public HelixManager getHelixZkManager() {
    return _helixZkManager;
  }

  public PinotResourceManagerResponse updateBrokerTenant(Tenant tenant) {
    PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    String brokerTenantTag = ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tenant.getTenantName());
    List<String> instancesInClusterWithTag =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTenantTag);
    if (instancesInClusterWithTag.size() > tenant.getNumberOfInstances()) {
      return scaleDownBroker(tenant, res, brokerTenantTag, instancesInClusterWithTag);
    }
    if (instancesInClusterWithTag.size() < tenant.getNumberOfInstances()) {
      return scaleUpBroker(tenant, res, brokerTenantTag, instancesInClusterWithTag);
    }
    res.status = ResponseStatus.success;
    return res;
  }

  private PinotResourceManagerResponse scaleUpBroker(Tenant tenant, PinotResourceManagerResponse res,
      String brokerTenantTag, List<String> instancesInClusterWithTag) {
    List<String> unTaggedInstanceList = getOnlineUnTaggedBrokerInstanceList();
    int numberOfInstancesToAdd = tenant.getNumberOfInstances() - instancesInClusterWithTag.size();
    if (unTaggedInstanceList.size() < numberOfInstancesToAdd) {
      res.status = ResponseStatus.failure;
      res.message =
          "Failed to allocate broker instances to Tag : " + tenant.getTenantName()
              + ", Current number of untagged broker instances : " + unTaggedInstanceList.size()
              + ", Current number of tagged broker instances : " + instancesInClusterWithTag.size()
              + ", Request asked number is : " + tenant.getNumberOfInstances();
      LOGGER.error(res.message);
      return res;
    }
    for (int i = 0; i < numberOfInstancesToAdd; ++i) {
      String instanceName = unTaggedInstanceList.get(i);
      retagInstance(instanceName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE, brokerTenantTag);
      // Update idealState by adding new instance to table mapping.
      addInstanceToBrokerIdealState(brokerTenantTag, instanceName);
    }
    res.status = ResponseStatus.success;
    return res;
  }

  public PinotResourceManagerResponse rebuildBrokerResourceFromHelixTags(final String tableName) {
    // Get the broker tag for this table
    String brokerTag = null;
    TenantConfig tenantConfig = null;

    try {
      final TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
      AbstractTableConfig tableConfig;
      if (tableType == TableType.OFFLINE) {
        tableConfig = ZKMetadataProvider.getOfflineTableConfig(getPropertyStore(), tableName);
      } else if (tableType == TableType.REALTIME) {
        tableConfig = ZKMetadataProvider.getRealtimeTableConfig(getPropertyStore(), tableName);
      } else {
        return new PinotResourceManagerResponse("Table " + tableName + " does not have a table type", false);
      }
      if (tableConfig == null) {
        return new PinotResourceManagerResponse("Table " + tableName + " does not exist", false);
      }
      tenantConfig = tableConfig.getTenantConfig();
    } catch (Exception e) {
      LOGGER.warn("Caught exception while getting tenant config for table {}", tableName, e);
      return new PinotResourceManagerResponse(
          "Failed to fetch broker tag for table " + tableName + " due to exception: " + e.getMessage(), false);
    }

    brokerTag = tenantConfig.getBroker();

    // Look for all instances tagged with this broker tag
    final Set<String> brokerInstances = getAllInstancesForBrokerTenant(brokerTag);

    // If we add a new broker, we want to rebuild the broker resource.
    HelixAdmin helixAdmin = getHelixAdmin();
    String clusterName = getHelixClusterName();
    IdealState brokerIdealState = HelixHelper.getBrokerIdealStates(helixAdmin, clusterName);

    Set<String> idealStateBrokerInstances = brokerIdealState.getInstanceSet(tableName);

    if(idealStateBrokerInstances.equals(brokerInstances)) {
      return new PinotResourceManagerResponse(
          "Broker resource is not rebuilt because ideal state is the same for table {} " + tableName, false);
    }

    // Reset ideal state with the instance list
    try {
      HelixHelper.updateIdealState(getHelixZkManager(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, new Function<IdealState, IdealState>() {
        @Nullable
        @Override
        public IdealState apply(@Nullable IdealState idealState) {
          Map<String, String> instanceStateMap = idealState.getInstanceStateMap(tableName);
          if (instanceStateMap != null) {
            instanceStateMap.clear();
          }

          for (String brokerInstance : brokerInstances) {
            idealState.setPartitionState(tableName, brokerInstance, BrokerOnlineOfflineStateModel.ONLINE);
          }

          return idealState;
        }
      }, DEFAULT_RETRY_POLICY);

      LOGGER.info("Successfully rebuilt brokerResource for table {}", tableName);
      return new PinotResourceManagerResponse("Rebuilt brokerResource for table " + tableName, true);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while rebuilding broker resource from Helix tags for table {}", e, tableName);
      return new PinotResourceManagerResponse(
          "Failed to rebuild brokerResource for table " + tableName + " due to exception: " + e.getMessage(), false);
    }
  }

  private void addInstanceToBrokerIdealState(String brokerTenantTag, String instanceName) {
    IdealState tableIdealState =
        _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    for (String tableName : tableIdealState.getPartitionSet()) {
      if (TableNameBuilder.getTableTypeFromTableName(tableName) == TableType.OFFLINE) {
        String brokerTag =
            ControllerTenantNameBuilder.getBrokerTenantNameForTenant(ZKMetadataProvider
                .getOfflineTableConfig(getPropertyStore(), tableName).getTenantConfig().getBroker());
        if (brokerTag.equals(brokerTenantTag)) {
          tableIdealState.setPartitionState(tableName, instanceName, BrokerOnlineOfflineStateModel.ONLINE);
        }
      } else if (TableNameBuilder.getTableTypeFromTableName(tableName) == TableType.REALTIME) {
        String brokerTag =
            ControllerTenantNameBuilder.getBrokerTenantNameForTenant(ZKMetadataProvider
                .getRealtimeTableConfig(getPropertyStore(), tableName).getTenantConfig().getBroker());
        if (brokerTag.equals(brokerTenantTag)) {
          tableIdealState.setPartitionState(tableName, instanceName, BrokerOnlineOfflineStateModel.ONLINE);
        }
      }
    }
    _helixAdmin.setResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE,
        tableIdealState);
  }

  private PinotResourceManagerResponse scaleDownBroker(Tenant tenant, PinotResourceManagerResponse res,
      String brokerTenantTag, List<String> instancesInClusterWithTag) {
    int numberBrokersToUntag = instancesInClusterWithTag.size() - tenant.getNumberOfInstances();
    for (int i = 0; i < numberBrokersToUntag; ++i) {
      retagInstance(instancesInClusterWithTag.get(i), brokerTenantTag, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }
    res.status = ResponseStatus.success;
    return res;
  }

  private void retagInstance(String instanceName, String oldTag, String newTag) {
    _helixAdmin.removeInstanceTag(_helixClusterName, instanceName, oldTag);
    _helixAdmin.addInstanceTag(_helixClusterName, instanceName, newTag);
  }

  public PinotResourceManagerResponse updateServerTenant(Tenant serverTenant) {
    PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    String realtimeServerTag = ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(serverTenant.getTenantName());
    List<String> taggedRealtimeServers = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, realtimeServerTag);
    String offlineServerTag = ControllerTenantNameBuilder.getOfflineTenantNameForTenant(serverTenant.getTenantName());
    List<String> taggedOfflineServers = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, offlineServerTag);
    Set<String> allServingServers = new HashSet<String>();
    allServingServers.addAll(taggedOfflineServers);
    allServingServers.addAll(taggedRealtimeServers);
    boolean isCurrentTenantColocated =
        (allServingServers.size() < taggedOfflineServers.size() + taggedRealtimeServers.size());
    if (isCurrentTenantColocated != serverTenant.isCoLocated()) {
      res.status = ResponseStatus.failure;
      res.message = "Not support different colocated type request for update request: " + serverTenant;
      LOGGER.error(res.message);
      return res;
    }
    if (serverTenant.getNumberOfInstances() < allServingServers.size()
        || serverTenant.getOfflineInstances() < taggedOfflineServers.size()
        || serverTenant.getRealtimeInstances() < taggedRealtimeServers.size()) {
      return scaleDownServer(serverTenant, res, taggedRealtimeServers, taggedOfflineServers, allServingServers);
    }
    return scaleUpServerTenant(serverTenant, res, realtimeServerTag, taggedRealtimeServers, offlineServerTag,
        taggedOfflineServers, allServingServers);
  }

  private PinotResourceManagerResponse scaleUpServerTenant(Tenant serverTenant, PinotResourceManagerResponse res,
      String realtimeServerTag, List<String> taggedRealtimeServers, String offlineServerTag,
      List<String> taggedOfflineServers, Set<String> allServingServers) {
    int incInstances = serverTenant.getNumberOfInstances() - allServingServers.size();
    List<String> unTaggedInstanceList = getOnlineUnTaggedServerInstanceList();
    if (unTaggedInstanceList.size() < incInstances) {
      res.status = ResponseStatus.failure;
      res.message =
          "Failed to allocate hardware resouces with tenant info: " + serverTenant
              + ", Current number of untagged instances : " + unTaggedInstanceList.size()
              + ", Current number of servering instances : " + allServingServers.size()
              + ", Current number of tagged offline server instances : " + taggedOfflineServers.size()
              + ", Current number of tagged realtime server instances : " + taggedRealtimeServers.size();
      LOGGER.error(res.message);
      return res;
    }
    if (serverTenant.isCoLocated()) {
      return updateColocatedServerTenant(serverTenant, res, realtimeServerTag, taggedRealtimeServers, offlineServerTag,
          taggedOfflineServers, incInstances, unTaggedInstanceList);
    } else {
      return updateIndependentServerTenant(serverTenant, res, realtimeServerTag, taggedRealtimeServers,
          offlineServerTag, taggedOfflineServers, incInstances, unTaggedInstanceList);
    }

  }

  private PinotResourceManagerResponse updateIndependentServerTenant(Tenant serverTenant,
      PinotResourceManagerResponse res, String realtimeServerTag, List<String> taggedRealtimeServers,
      String offlineServerTag, List<String> taggedOfflineServers, int incInstances, List<String> unTaggedInstanceList) {
    int incOffline = serverTenant.getOfflineInstances() - taggedOfflineServers.size();
    int incRealtime = serverTenant.getRealtimeInstances() - taggedRealtimeServers.size();
    for (int i = 0; i < incOffline; ++i) {
      retagInstance(unTaggedInstanceList.get(i), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
    }
    for (int i = incOffline; i < incOffline + incRealtime; ++i) {
      String instanceName = unTaggedInstanceList.get(i);
      retagInstance(instanceName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, realtimeServerTag);
      // TODO: update idealStates & instanceZkMetadata
    }
    res.status = ResponseStatus.success;
    return res;
  }

  private PinotResourceManagerResponse updateColocatedServerTenant(Tenant serverTenant,
      PinotResourceManagerResponse res, String realtimeServerTag, List<String> taggedRealtimeServers,
      String offlineServerTag, List<String> taggedOfflineServers, int incInstances, List<String> unTaggedInstanceList) {
    int incOffline = serverTenant.getOfflineInstances() - taggedOfflineServers.size();
    int incRealtime = serverTenant.getRealtimeInstances() - taggedRealtimeServers.size();
    taggedRealtimeServers.removeAll(taggedOfflineServers);
    taggedOfflineServers.removeAll(taggedRealtimeServers);
    for (int i = 0; i < incOffline; ++i) {
      if (i < incInstances) {
        retagInstance(unTaggedInstanceList.get(i), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
      } else {
        _helixAdmin.addInstanceTag(_helixClusterName, taggedRealtimeServers.get(i - incInstances), offlineServerTag);
      }
    }
    for (int i = incOffline; i < incOffline + incRealtime; ++i) {
      if (i < incInstances) {
        retagInstance(unTaggedInstanceList.get(i), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, realtimeServerTag);
        // TODO: update idealStates & instanceZkMetadata
      } else {
        _helixAdmin.addInstanceTag(_helixClusterName, taggedOfflineServers.get(i - Math.max(incInstances, incOffline)),
            realtimeServerTag);
        // TODO: update idealStates & instanceZkMetadata
      }
    }
    res.status = ResponseStatus.success;
    return res;
  }

  private PinotResourceManagerResponse scaleDownServer(Tenant serverTenant, PinotResourceManagerResponse res,
      List<String> taggedRealtimeServers, List<String> taggedOfflineServers, Set<String> allServingServers) {
    res.status = ResponseStatus.failure;
    res.message =
        "Not support to size down the current server cluster with tenant info: " + serverTenant
            + ", Current number of servering instances : " + allServingServers.size()
            + ", Current number of tagged offline server instances : " + taggedOfflineServers.size()
            + ", Current number of tagged realtime server instances : " + taggedRealtimeServers.size();
    LOGGER.error(res.message);
    return res;
  }

  public boolean isTenantExisted(String tenantName) {
    if (!_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tenantName)).isEmpty()) {
      return true;
    }
    if (!_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getOfflineTenantNameForTenant(tenantName)).isEmpty()) {
      return true;
    }
    if (!_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(tenantName)).isEmpty()) {
      return true;
    }
    return false;
  }

  public boolean isBrokerTenantDeletable(String tenantName) {
    String brokerTag = ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tenantName);
    Set<String> taggedInstances =
        new HashSet<String>(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTag));
    String brokerName = CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;
    IdealState brokerIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, brokerName);
    for (String partition : brokerIdealState.getPartitionSet()) {
      for (String instance : brokerIdealState.getInstanceSet(partition)) {
        if (taggedInstances.contains(instance)) {
          return false;
        }
      }
    }
    return true;
  }

  public boolean isServerTenantDeletable(String tenantName) {
    Set<String> taggedInstances =
        new HashSet<String>(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
            ControllerTenantNameBuilder.getOfflineTenantNameForTenant(tenantName)));
    taggedInstances.addAll(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(tenantName)));
    for (String tableName : getAllTableNames()) {
      if (tableName.equals(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)) {
        continue;
      }
      IdealState tableIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
      for (String partition : tableIdealState.getPartitionSet()) {
        for (String instance : tableIdealState.getInstanceSet(partition)) {
          if (taggedInstances.contains(instance)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  public Set<String> getAllBrokerTenantNames() {
    Set<String> tenantSet = new HashSet<String>();
    List<String> instancesInCluster = _helixAdmin.getInstancesInCluster(_helixClusterName);
    for (String instanceName : instancesInCluster) {
      InstanceConfig config = _helixDataAccessor.getProperty(_keyBuilder.instanceConfig(instanceName));
      for (String tag : config.getTags()) {
        if (tag.equals(CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
            || tag.equals(CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)) {
          continue;
        }
        if (ControllerTenantNameBuilder.getTenantRoleFromTenantName(tag) == TenantRole.BROKER) {
          tenantSet.add(ControllerTenantNameBuilder.getExternalTenantName(tag));
        }
      }
    }
    return tenantSet;
  }

  public Set<String> getAllServerTenantNames() {
    Set<String> tenantSet = new HashSet<String>();
    List<String> instancesInCluster = _helixAdmin.getInstancesInCluster(_helixClusterName);
    for (String instanceName : instancesInCluster) {
      InstanceConfig config = _helixDataAccessor.getProperty(_keyBuilder.instanceConfig(instanceName));
      for (String tag : config.getTags()) {
        if (tag.equals(CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
            || tag.equals(CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)) {
          continue;
        }
        if (ControllerTenantNameBuilder.getTenantRoleFromTenantName(tag) == TenantRole.SERVER) {
          tenantSet.add(ControllerTenantNameBuilder.getExternalTenantName(tag));
        }
      }
    }
    return tenantSet;
  }

  private List<String> getTagsForInstance(String instanceName) {
    InstanceConfig config = _helixDataAccessor.getProperty(_keyBuilder.instanceConfig(instanceName));
    return config.getTags();
  }

  public PinotResourceManagerResponse createServerTenant(Tenant serverTenant) {
    PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    int numberOfInstances = serverTenant.getNumberOfInstances();
    List<String> unTaggedInstanceList = getOnlineUnTaggedServerInstanceList();
    if (unTaggedInstanceList.size() < numberOfInstances) {
      res.status = ResponseStatus.failure;
      res.message =
          "Failed to allocate server instances to Tag : " + serverTenant.getTenantName()
              + ", Current number of untagged server instances : " + unTaggedInstanceList.size()
              + ", Request asked number is : " + serverTenant.getNumberOfInstances();
      LOGGER.error(res.message);
      return res;
    } else {
      if (serverTenant.isCoLocated()) {
        assignColocatedServerTenant(serverTenant, numberOfInstances, unTaggedInstanceList);
      } else {
        assignIndependentServerTenant(serverTenant, numberOfInstances, unTaggedInstanceList);
      }
    }
    res.status = ResponseStatus.success;
    return res;
  }

  private void assignIndependentServerTenant(Tenant serverTenant, int numberOfInstances,
      List<String> unTaggedInstanceList) {
    String offlineServerTag = ControllerTenantNameBuilder.getOfflineTenantNameForTenant(serverTenant.getTenantName());
    for (int i = 0; i < serverTenant.getOfflineInstances(); i++) {
      retagInstance(unTaggedInstanceList.get(i), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
    }
    String realtimeServerTag = ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(serverTenant.getTenantName());
    for (int i = 0; i < serverTenant.getRealtimeInstances(); i++) {
      retagInstance(unTaggedInstanceList.get(i + serverTenant.getOfflineInstances()),
          CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, realtimeServerTag);
    }
  }

  private void assignColocatedServerTenant(Tenant serverTenant, int numberOfInstances, List<String> unTaggedInstanceList) {
    int cnt = 0;
    String offlineServerTag = ControllerTenantNameBuilder.getOfflineTenantNameForTenant(serverTenant.getTenantName());
    for (int i = 0; i < serverTenant.getOfflineInstances(); i++) {
      retagInstance(unTaggedInstanceList.get(cnt++), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
    }
    String realtimeServerTag = ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(serverTenant.getTenantName());
    for (int i = 0; i < serverTenant.getRealtimeInstances(); i++) {
      retagInstance(unTaggedInstanceList.get(cnt++), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, realtimeServerTag);
      if (cnt == numberOfInstances) {
        cnt = 0;
      }
    }
  }

  public PinotResourceManagerResponse createBrokerTenant(Tenant brokerTenant) {
    PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    List<String> unTaggedInstanceList = getOnlineUnTaggedBrokerInstanceList();
    int numberOfInstances = brokerTenant.getNumberOfInstances();
    if (unTaggedInstanceList.size() < numberOfInstances) {
      res.status = ResponseStatus.failure;
      res.message =
          "Failed to allocate broker instances to Tag : " + brokerTenant.getTenantName()
              + ", Current number of untagged server instances : " + unTaggedInstanceList.size()
              + ", Request asked number is : " + brokerTenant.getNumberOfInstances();
      LOGGER.error(res.message);
      return res;
    }
    String brokerTag = ControllerTenantNameBuilder.getBrokerTenantNameForTenant(brokerTenant.getTenantName());
    for (int i = 0; i < brokerTenant.getNumberOfInstances(); ++i) {
      retagInstance(unTaggedInstanceList.get(i), CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE, brokerTag);
    }
    res.status = ResponseStatus.success;
    return res;

  }

  public PinotResourceManagerResponse deleteOfflineServerTenantFor(String tenantName) {
    PinotResourceManagerResponse response = new PinotResourceManagerResponse();
    String offlineTenantTag = ControllerTenantNameBuilder.getOfflineTenantNameForTenant(tenantName);
    List<String> instancesInClusterWithTag =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, offlineTenantTag);
    for (String instanceName : instancesInClusterWithTag) {
      _helixAdmin.removeInstanceTag(_helixClusterName, instanceName, offlineTenantTag);
      if (getTagsForInstance(instanceName).isEmpty()) {
        _helixAdmin.addInstanceTag(_helixClusterName, instanceName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      }
    }
    response.status = ResponseStatus.success;
    return response;
  }

  public PinotResourceManagerResponse deleteRealtimeServerTenantFor(String tenantName) {
    PinotResourceManagerResponse response = new PinotResourceManagerResponse();
    String realtimeTenantTag = ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(tenantName);
    List<String> instancesInClusterWithTag =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, realtimeTenantTag);
    for (String instanceName : instancesInClusterWithTag) {
      _helixAdmin.removeInstanceTag(_helixClusterName, instanceName, realtimeTenantTag);
      if (getTagsForInstance(instanceName).isEmpty()) {
        _helixAdmin.addInstanceTag(_helixClusterName, instanceName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      }
    }
    response.status = ResponseStatus.success;
    return response;
  }

  public PinotResourceManagerResponse deleteBrokerTenantFor(String tenantName) {
    PinotResourceManagerResponse response = new PinotResourceManagerResponse();
    String brokerTag = ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tenantName);
    List<String> instancesInClusterWithTag = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTag);
    for (String instance : instancesInClusterWithTag) {
      retagInstance(instance, brokerTag, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }
    return response;
  }

  public Set<String> getAllInstancesForServerTenant(String tenantName) {
    Set<String> instancesSet = new HashSet<String>();
    instancesSet.addAll(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getOfflineTenantNameForTenant(tenantName)));
    instancesSet.addAll(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(tenantName)));
    return instancesSet;
  }

  public Set<String> getAllInstancesForBrokerTenant(String tenantName) {
    Set<String> instancesSet = new HashSet<String>();
    instancesSet.addAll(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tenantName)));
    return instancesSet;
  }

  /**
   * API 2.0
   */

  /**
   * Schema APIs
   */
  /**
   *
   * @param schema
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   */
  public void addOrUpdateSchema(Schema schema) throws IllegalArgumentException, IllegalAccessException {
    ZNRecord record = SchemaUtils.toZNRecord(schema);
    String name = schema.getSchemaName();
    PinotHelixPropertyStoreZnRecordProvider propertyStoreHelper =
        PinotHelixPropertyStoreZnRecordProvider.forSchema(_propertyStore);
    propertyStoreHelper.set(name, record);
  }

  /**
   * Delete the given schema.
   * @param schema The schema to be deleted.
   * @return True on success, false otherwise.
   */
  public boolean deleteSchema(Schema schema) {
    if (schema != null) {
      String propertyStorePath = ZKMetadataProvider.constructPropertyStorePathForSchema(schema.getSchemaName());
      if (_propertyStore.exists(propertyStorePath, AccessOption.PERSISTENT)) {
        _propertyStore.remove(propertyStorePath, AccessOption.PERSISTENT);
        return true;
      }
    }
    return false;
  }
  /**
   *
   * @param schemaName
   * @return
   * @throws JsonParseException
   * @throws JsonMappingException
   * @throws IOException
   */
  public @Nullable Schema getSchema(String schemaName) throws JsonParseException, JsonMappingException, IOException {
    PinotHelixPropertyStoreZnRecordProvider propertyStoreHelper =
        PinotHelixPropertyStoreZnRecordProvider.forSchema(_propertyStore);
    ZNRecord record = propertyStoreHelper.get(schemaName);
    return record != null ? SchemaUtils.fromZNRecord(record) : null;
  }

  /**
   *
   * @return
   */
  public List<String> getSchemaNames() {
    return _propertyStore.getChildNames(PinotHelixPropertyStoreZnRecordProvider.forSchema(_propertyStore)
        .getRelativePath(), AccessOption.PERSISTENT);
  }

  /**
   * Table APIs
   */

  public void addTable(AbstractTableConfig config) throws JsonGenerationException, JsonMappingException, IOException {
    TenantConfig tenantConfig = null;
    TableType type = TableType.valueOf(config.getTableType().toUpperCase());
    if (isSingleTenantCluster()) {
      tenantConfig = new TenantConfig();
      tenantConfig.setBroker(ControllerTenantNameBuilder
          .getBrokerTenantNameForTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
      switch (type) {
        case OFFLINE:
          tenantConfig.setServer(ControllerTenantNameBuilder
              .getOfflineTenantNameForTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
          break;
        case REALTIME:
          tenantConfig.setServer(ControllerTenantNameBuilder
              .getRealtimeTenantNameForTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
          break;
        default:
          throw new RuntimeException("UnSupported table type");
      }
      config.setTenantConfig(tenantConfig);
    } else {
      tenantConfig = config.getTenantConfig();
      if (tenantConfig.getBroker() == null || tenantConfig.getServer() == null) {
        throw new RuntimeException("missing tenant configs");
      }
    }

    SegmentsValidationAndRetentionConfig segmentsConfig = config.getValidationConfig();
    switch (type) {
      case OFFLINE:
        final String offlineTableName = config.getTableName();

        // now lets build an ideal state
        LOGGER.info("building empty ideal state for table : " + offlineTableName);
        final IdealState offlineIdealState =
            PinotTableIdealStateBuilder.buildEmptyIdealStateFor(offlineTableName,
                Integer.parseInt(segmentsConfig.getReplication()));
        LOGGER.info("adding table via the admin");
        _helixAdmin.addResource(_helixClusterName, offlineTableName, offlineIdealState);
        LOGGER.info("successfully added the table : " + offlineTableName + " to the cluster");

        // lets add table configs
        ZKMetadataProvider.setOfflineTableConfig(_propertyStore, offlineTableName,
            AbstractTableConfig.toZnRecord(config));

        _propertyStore.create(ZKMetadataProvider.constructPropertyStorePathForResource(offlineTableName), new ZNRecord(
            offlineTableName), AccessOption.PERSISTENT);
        break;
      case REALTIME:
        final String realtimeTableName = config.getTableName();
        // lets add table configs

        ZKMetadataProvider.setRealtimeTableConfig(_propertyStore, realtimeTableName, AbstractTableConfig.toZnRecord(config));
        /*
         * PinotRealtimeSegmentManager sets up watches on table and segment path. When a table gets created,
         * it expects the INSTANCE path in propertystore to be set up so that it can get the kafka group ID and
         * create (high-level consumer) segments for that table.
         * So, we need to set up the instance first, before adding the table resource for HLC new table creation.
         *
         * For low-level consumers, the order is to create the resource first, and set up the propertystore with segments
         * and then tweak the idealstate to add those segments.
         *
         * We also need to support the case when a high-level consumer already exists for a table and we are adding
         * the low-level consumers.
         */
        IndexingConfig indexingConfig = config.getIndexingConfig();
        ensureRealtimeClusterIsSetUp(config, realtimeTableName, indexingConfig);

        LOGGER.info("Successfully added or updated the table {} ", realtimeTableName);
        break;
      default:
        throw new RuntimeException("UnSupported table type");
    }
    handleBrokerResource(config);
  }

  private void ensureRealtimeClusterIsSetUp(AbstractTableConfig config, String realtimeTableName,
      IndexingConfig indexingConfig) {
    KafkaStreamMetadata kafkaStreamMetadata = new KafkaStreamMetadata(indexingConfig.getStreamConfigs());
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, realtimeTableName);

    if (kafkaStreamMetadata.hasHighLevelKafkaConsumerType()) {
     if (kafkaStreamMetadata.hasSimpleKafkaConsumerType()) {
       // We may be adding on low-level, or creating both.
       if (idealState == null) {
         // Need to create both. Create high-level consumer first.
         createHelixEntriesForHighLevelConsumer(config, realtimeTableName, idealState);
         idealState = _helixAdmin.getResourceIdealState(_helixClusterName, realtimeTableName);
         LOGGER.info("Configured new HLC for table {}", realtimeTableName);
       }
       // Fall through to create low-level consumers
     } else {
       // Only high-level consumer specified in the config.
       createHelixEntriesForHighLevelConsumer(config, realtimeTableName, idealState);
       // Clean up any LLC table if they are present
       PinotLLCRealtimeSegmentManager.getInstance().cleanupLLC(realtimeTableName);
     }
    }

    // Either we have only low-level consumer, or both.
    if (kafkaStreamMetadata.hasSimpleKafkaConsumerType()) {
      // Will either create idealstate entry, or update the IS entry with new segments
      // (unless there are low-level segments already present)
      final String llcKafkaPartitionAssignmentPath = ZKMetadataProvider.constructPropertyStorePathForKafkaPartitions(realtimeTableName);
      if(!_propertyStore.exists(llcKafkaPartitionAssignmentPath, AccessOption.PERSISTENT)) {
        PinotTableIdealStateBuilder.buildLowLevelRealtimeIdealStateFor(realtimeTableName, config, _helixAdmin,
            _helixClusterName, idealState);
        LOGGER.info("Successfully added Helix entries for low-level consumers for {} ", realtimeTableName);
      } else {
        LOGGER.info("LLC is already set up for table {}, not configuring again", realtimeTableName);
      }
    }
  }

  private void createHelixEntriesForHighLevelConsumer(AbstractTableConfig config, String realtimeTableName,
      IdealState idealState) {
    if (idealState == null) {
      idealState = PinotTableIdealStateBuilder
          .buildInitialHighLevelRealtimeIdealStateFor(realtimeTableName, config, _helixAdmin, _helixClusterName,
              _propertyStore);
      LOGGER.info("Adding helix resource with empty HLC IdealState for {}", realtimeTableName);
      _helixAdmin.addResource(_helixClusterName, realtimeTableName, idealState);
    } else {
      // TODO jfim: We get in this block if we're trying to add a HLC or it already exists. If it doesn't already exist, we need to set instance configs properly (which is done in buildInitialHighLevelRealtimeIdealState, surprisingly enough). For now, do nothing.
      LOGGER.info("Not reconfiguring HLC for table {}", realtimeTableName);
    }
    LOGGER.info("Successfully created empty ideal state for  high level consumer for {} ", realtimeTableName);
    // Finally, create the propertystore entry that will trigger watchers to create segments
    String tablePropertyStorePath = ZKMetadataProvider.constructPropertyStorePathForResource(realtimeTableName);
    if (!_propertyStore.exists(tablePropertyStorePath, AccessOption.PERSISTENT)) {
      _propertyStore.create(tablePropertyStorePath, new ZNRecord(realtimeTableName), AccessOption.PERSISTENT);
    }
  }

  public void setTableConfig(AbstractTableConfig config, String tableNameWithSuffix, TableType type)
      throws JsonGenerationException, JsonMappingException, IOException {
    if (type == TableType.REALTIME) {
      ZKMetadataProvider.setRealtimeTableConfig(_propertyStore, tableNameWithSuffix,
          AbstractTableConfig.toZnRecord(config));
      ensureRealtimeClusterIsSetUp(config, tableNameWithSuffix, config.getIndexingConfig());
    } else {
      ZKMetadataProvider.setOfflineTableConfig(_propertyStore, tableNameWithSuffix,
          AbstractTableConfig.toZnRecord(config));
    }

  }

  public void updateMetadataConfigFor(String tableName, TableType type, TableCustomConfig newConfigs) throws Exception {
    String actualTableName = new TableNameBuilder(type).forTable(tableName);
    AbstractTableConfig config;
    if (type == TableType.REALTIME) {
      config = ZKMetadataProvider.getRealtimeTableConfig(getPropertyStore(), actualTableName);
    } else {
      config = ZKMetadataProvider.getOfflineTableConfig(getPropertyStore(), actualTableName);
    }
    if (config == null) {
      throw new RuntimeException("tableName : " + tableName + " of type : " + type + " not found");
    }
    config.setCustomConfigs(newConfigs);
    setTableConfig(config, actualTableName, type);
  }

  public void updateSegmentsValidationAndRetentionConfigFor(String tableName, TableType type,
      SegmentsValidationAndRetentionConfig newConfigs) throws Exception {
    String actualTableName = new TableNameBuilder(type).forTable(tableName);
    AbstractTableConfig config;
    if (type == TableType.REALTIME) {
      config = ZKMetadataProvider.getRealtimeTableConfig(getPropertyStore(), actualTableName);
    } else {
      config = ZKMetadataProvider.getOfflineTableConfig(getPropertyStore(), actualTableName);
    }
    if (config == null) {
      throw new RuntimeException("tableName : " + tableName + " of type : " + type + " not found");
    }
    config.setValidationConfig(newConfigs);

    setTableConfig(config, actualTableName, type);
  }

  public void updateIndexingConfigFor(String tableName, TableType type, IndexingConfig newConfigs) throws Exception {
    String actualTableName = new TableNameBuilder(type).forTable(tableName);
    AbstractTableConfig config;
    if (type == TableType.REALTIME) {
      config = ZKMetadataProvider.getRealtimeTableConfig(getPropertyStore(), actualTableName);
      if (config != null) {
        ((RealtimeTableConfig) config).setIndexConfig(newConfigs);
      }
    } else {
      config = ZKMetadataProvider.getOfflineTableConfig(getPropertyStore(), actualTableName);
      if (config != null) {
        ((OfflineTableConfig) config).setIndexConfig(newConfigs);
      }
    }
    if (config == null) {
      throw new RuntimeException("tableName : " + tableName + " of type : " + type + " not found");
    }

    setTableConfig(config, actualTableName, type);

    if (type == TableType.REALTIME) {
      ensureRealtimeClusterIsSetUp(config, tableName, newConfigs);
    }
  }

  private void handleBrokerResource(AbstractTableConfig tableConfig) {
    try {
      final String brokerTenant =
          ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tableConfig.getTenantConfig().getBroker());
      if (_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTenant).isEmpty()) {
        throw new RuntimeException("broker tenant : " + tableConfig.getTenantConfig().getBroker() + " does not exist");
      }
      LOGGER.info("Trying to update BrokerDataResource IdealState!");
      final String tableName = tableConfig.getTableName();
      HelixHelper.updateIdealState(_helixZkManager, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, new Function<IdealState, IdealState>() {
        @Nullable
        @Override
        public IdealState apply(@Nullable IdealState idealState) {
          if (idealState == null) {
            throw new RuntimeException("idealstate not set up for " + CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
          }
          for (String instanceName : _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTenant)) {
            idealState.setPartitionState(tableName, instanceName, BrokerOnlineOfflineStateModel.ONLINE);
          }
          return idealState;
        }
      }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
    } catch (final Exception e) {
      LOGGER.warn("Caught exception while creating broker", e);
    }
  }

  public void deleteOfflineTable(String tableName) {
    final String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
    // Remove from brokerResource
    HelixHelper.removeResourceFromBrokerIdealState(_helixZkManager, offlineTableName);
    // Delete data table
    if (!_helixAdmin.getResourcesInCluster(_helixClusterName).contains(offlineTableName)) {
      return;
    }
    // remove from property store
    ZKMetadataProvider.removeResourceSegmentsFromPropertyStore(getPropertyStore(), offlineTableName);
    ZKMetadataProvider.removeResourceConfigFromPropertyStore(getPropertyStore(), offlineTableName);

    // dropping table
    _helixAdmin.dropResource(_helixClusterName, offlineTableName);
  }

  public void deleteRealtimeTable(String tableName) {
    final String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
    // Remove from brokerResource
    HelixHelper.removeResourceFromBrokerIdealState(_helixZkManager, realtimeTableName);
    // Delete data table
    if (!_helixAdmin.getResourcesInCluster(_helixClusterName).contains(realtimeTableName)) {
      return;
    }
    // remove from property store
    ZKMetadataProvider.removeResourceSegmentsFromPropertyStore(getPropertyStore(), realtimeTableName);
    ZKMetadataProvider.removeResourceConfigFromPropertyStore(getPropertyStore(), realtimeTableName);
    ZKMetadataProvider.removeKafkaPartitionAssignmentFromPropertyStore(getPropertyStore(), realtimeTableName);
    // Remove groupId/PartitionId mapping for realtime table type.
    for (String instance : getAllInstancesForTable(realtimeTableName)) {
      InstanceZKMetadata instanceZKMetadata = ZKMetadataProvider.getInstanceZKMetadata(getPropertyStore(), instance);
      if (instanceZKMetadata != null) {
        instanceZKMetadata.removeResource(realtimeTableName);
        ZKMetadataProvider.setInstanceZKMetadata(getPropertyStore(), instanceZKMetadata);
      }
    }

    // dropping table
    _helixAdmin.dropResource(_helixClusterName, realtimeTableName);
  }

  /**
   * Toggle the status of the table between OFFLINE and ONLINE.
   *
   * @param tableName: Name of the table for which to toggle the status.
   * @param status: True for ONLINE and False for OFFLINE.
   * @return
   */
  public PinotResourceManagerResponse toggleTableState(String tableName, boolean status) {
    if (!_helixAdmin.getResourcesInCluster(_helixClusterName).contains(tableName)) {
      return new PinotResourceManagerResponse("Error: Table " + tableName + " not found.", false);
    }
    _helixAdmin.enableResource(_helixClusterName, tableName, status);
    return (status) ? new PinotResourceManagerResponse("Table " + tableName + " successfully enabled.", true)
        : new PinotResourceManagerResponse("Table " + tableName + " successfully disabled.", true);
  }

  /**
   * Drop the table from helix cluster.
   *
   * @param tableName: Name of table to be dropped.
   * @return
   */
  public PinotResourceManagerResponse dropTable(String tableName) {
    if (!_helixAdmin.getResourcesInCluster(_helixClusterName).contains(tableName)) {
      return new PinotResourceManagerResponse("Error: Table " + tableName + " not found.", false);
    }

    if (getAllSegmentsForResource(tableName).size() != 0) {
      return new PinotResourceManagerResponse("Error: Table " + tableName + " has segments, drop them first.", false);
    }

    _helixAdmin.dropResource(_helixClusterName, tableName);

    // remove from property store
    ZKMetadataProvider.removeResourceSegmentsFromPropertyStore(getPropertyStore(), tableName);
    ZKMetadataProvider.removeResourceConfigFromPropertyStore(getPropertyStore(), tableName);

    return new PinotResourceManagerResponse("Table " + tableName + " successfully dropped.", true);
  }

  private Set<String> getAllInstancesForTable(String tableName) {
    Set<String> instanceSet = new HashSet<String>();
    IdealState tableIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    for (String partition : tableIdealState.getPartitionSet()) {
      instanceSet.addAll(tableIdealState.getInstanceSet(partition));
    }
    return instanceSet;
  }

  public PinotResourceManagerResponse addSegment(final SegmentMetadata segmentMetadata, String downloadUrl) {
    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    String segmentName = "Unknown";
    String tableName = "Unknown";
    try {
      if (!matchTableName(segmentMetadata)) {
        throw new RuntimeException("Reject segment: table name is not registered." + " table name: "
            + segmentMetadata.getTableName() + "\n");
      }
      segmentName = segmentMetadata.getName();
      tableName = segmentMetadata.getTableName();

      if (ifSegmentExisted(segmentMetadata)) {
        if (ifRefreshAnExistedSegment(segmentMetadata, segmentName, tableName)) {
          OfflineSegmentZKMetadata offlineSegmentZKMetadata =
              ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, segmentMetadata.getTableName(),
                  segmentMetadata.getName());
          offlineSegmentZKMetadata = ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
          offlineSegmentZKMetadata.setDownloadUrl(downloadUrl);
          offlineSegmentZKMetadata.setRefreshTime(System.currentTimeMillis());
          ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata);
          LOGGER.info("Refresh segment {} of table {} to propertystore ", segmentName, tableName);
          boolean success = true;
          if (shouldSendMessage(offlineSegmentZKMetadata)) {
            // Send a message to the servers to update the segment.
            // We return success even if we are not able to send messages (which can happen if no servers are alive).
            // For segment validation errors we would have returned earlier.
            sendSegmentRefreshMessage(offlineSegmentZKMetadata);
          } else {
            // Go through the ONLINE->OFFLINE->ONLINE state transition to update the segment
            success = updateExistedSegment(offlineSegmentZKMetadata);
          }
          if (success) {
            res.status = ResponseStatus.success;
          } else {
            LOGGER.error("Failed to refresh segment {} of table {}, marking crc and creation time as invalid",
                segmentName, tableName);
            offlineSegmentZKMetadata.setCrc(-1L);
            offlineSegmentZKMetadata.setCreationTime(-1L);
            ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata);
          }
        } else {
          String msg =
              "Not refreshing identical segment " + segmentName + "of table " + tableName + " with creation time "
                  + segmentMetadata.getIndexCreationTime() + " and crc " + segmentMetadata.getCrc();
          LOGGER.info(msg);
          res.status = ResponseStatus.success;
          res.message = msg;
        }
      } else {
        OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
        offlineSegmentZKMetadata = ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
        offlineSegmentZKMetadata.setDownloadUrl(downloadUrl);
        offlineSegmentZKMetadata.setPushTime(System.currentTimeMillis());
        ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata);
        LOGGER.info("Added segment {} of table {} to propertystore", segmentName, tableName);

        addNewOfflineSegment(segmentMetadata);
        res.status = ResponseStatus.success;
      }
    } catch (final Exception e) {
      LOGGER.error("Caught exception while adding segment {} of table {}", segmentName, tableName, e);
      res.status = ResponseStatus.failure;
      res.message = e.getMessage();
    }

    return res;
  }

  // Check to see if the table has been explicitly configured to NOT use messageBasedRefresh.
  private boolean shouldSendMessage(OfflineSegmentZKMetadata segmentZKMetadata) {
    final String rawTableName = segmentZKMetadata.getTableName();
    AbstractTableConfig tableConfig = ZKMetadataProvider.getOfflineTableConfig(_propertyStore, rawTableName);
    TableCustomConfig customConfig = tableConfig.getCustomConfigs();
    if (customConfig != null) {
      Map<String, String> customConfigMap = customConfig.getCustomConfigs();
      if (customConfigMap != null) {
        if (customConfigMap.containsKey(TableCustomConfig.MESSAGE_BASED_REFRESH_KEY) &&
            ! Boolean.valueOf(customConfigMap.get(TableCustomConfig.MESSAGE_BASED_REFRESH_KEY))) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Attempt to send a message to refresh the new segment. We do not wait for any acknowledgements.
   * The message is sent as session-specific, so if a new zk session is created (e.g. server restarts)
   * it will not get the message.
   *
   * @param segmentZKMetadata is the metadata of the newly arrived segment.
   * @return true if message has been sent to at least one instance of the server hosting the segment.
   */
  private void sendSegmentRefreshMessage(OfflineSegmentZKMetadata segmentZKMetadata) {
    final String segmentName = segmentZKMetadata.getSegmentName();
    final String rawTableName = segmentZKMetadata.getTableName();
    final String tableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(rawTableName);
    final int timeoutMs = -1; // Infinite timeout on the recipient.

    SegmentRefreshMessage refreshMessage = new SegmentRefreshMessage(tableName, segmentName, segmentZKMetadata.getCrc());

    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource(tableName);
    recipientCriteria.setPartition(segmentName);
    recipientCriteria.setSessionSpecific(true);

    ClusterMessagingService messagingService = _helixZkManager.getMessagingService();
    LOGGER.info("Sending refresh message for segment {} of table {}:{} to recipients {}", segmentName,
        rawTableName, refreshMessage, recipientCriteria);
    // Helix sets the timeoutMs argument specified in 'send' call as the processing timeout of the message.
    int nMsgsSent = messagingService.send(recipientCriteria, refreshMessage, null, timeoutMs);
    if (nMsgsSent > 0) {
      // TODO Would be nice if we can get the name of the instances to which messages were sent.
      LOGGER.info("Sent {} msgs to refresh segment {} of table {}", nMsgsSent, segmentName, rawTableName);
    } else {
      // May be the case when none of the servers are up yet. That is OK, because when they come up they will get the
      // new version of the segment.
      LOGGER.warn("Unable to send segment refresh message for {} of table {}, nMsgs={}", segmentName, tableName, nMsgsSent);
    }
  }

  /**
   * Helper method to add the passed in offline segment to the helix cluster.
   * - Gets the segment name and the table name from the passed in segment meta-data.
   * - Identifies the instance set onto which the segment needs to be added, based on
   *   segment assignment strategy and replicas in the table config in the property-store.
   * - Updates ideal state such that the new segment is assigned to required set of instances as per
   *    the segment assignment strategy and replicas.
   *
   * @param segmentMetadata Meta-data for the segment, used to access segmentName and tableName.
   * @throws JsonParseException
   * @throws JsonMappingException
   * @throws JsonProcessingException
   * @throws JSONException
   * @throws IOException
   */
  private void addNewOfflineSegment(final SegmentMetadata segmentMetadata) throws JsonParseException,
      JsonMappingException, JsonProcessingException, JSONException, IOException {
    final AbstractTableConfig offlineTableConfig =
        ZKMetadataProvider.getOfflineTableConfig(_propertyStore, segmentMetadata.getTableName());

    final String segmentName = segmentMetadata.getName();
    final String offlineTableName =
        TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(segmentMetadata.getTableName());

    if (!SEGMENT_ASSIGNMENT_STRATEGY_MAP.containsKey(offlineTableName)) {
      SEGMENT_ASSIGNMENT_STRATEGY_MAP.put(offlineTableName, SegmentAssignmentStrategyFactory
          .getSegmentAssignmentStrategy(offlineTableConfig.getValidationConfig().getSegmentAssignmentStrategy()));
    }
    final SegmentAssignmentStrategy segmentAssignmentStrategy = SEGMENT_ASSIGNMENT_STRATEGY_MAP.get(offlineTableName);

    // Passing a callable to this api to avoid helixHelper having which is in pinot-common having to
    // depend upon pinot-controller.
    Callable<List<String>> getInstancesForSegment = new Callable<List<String>>() {
      @Override
      public List<String> call() throws Exception {
        final IdealState currentIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, offlineTableName);
        final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(segmentName);

        if (currentInstanceSet.isEmpty()) {
          final String serverTenant =
              ControllerTenantNameBuilder.getOfflineTenantNameForTenant(offlineTableConfig.getTenantConfig()
                  .getServer());
          final int replicas = Integer.parseInt(offlineTableConfig.getValidationConfig().getReplication());
          return segmentAssignmentStrategy.getAssignedInstances(_helixAdmin, _helixClusterName, segmentMetadata,
              replicas, serverTenant);
        } else {
          return new ArrayList<String>(currentIdealState.getInstanceSet(segmentName));
        }
      }
    };

    HelixHelper.addSegmentToIdealState(_helixZkManager, offlineTableName, segmentName, getInstancesForSegment);
  }

  /**
   * Returns true if the table name specified in the segment meta data has a corresponding
   * realtime or offline table in the helix cluster
   *
   * @param segmentMetadata Meta-data for the segment
   * @return
   */
  private boolean matchTableName(SegmentMetadata segmentMetadata) {
    if (segmentMetadata == null || segmentMetadata.getTableName() == null) {
      LOGGER.error("SegmentMetadata or table name is null");
      return false;
    }
    if ("realtime".equalsIgnoreCase(segmentMetadata.getIndexType())) {
      if (getAllTableNames().contains(
          TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(segmentMetadata.getTableName()))) {
        return true;
      }
    } else {
      if (getAllTableNames().contains(
          TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(segmentMetadata.getTableName()))) {
        return true;
      }
    }
    LOGGER.error("Table {} is not registered", segmentMetadata.getTableName());
    return false;
  }

  private boolean updateExistedSegment(SegmentZKMetadata segmentZKMetadata) {
    final String tableName;
    if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
      tableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(segmentZKMetadata.getTableName());
    } else {
      tableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(segmentZKMetadata.getTableName());
    }
    final String segmentName = segmentZKMetadata.getSegmentName();

    HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    PropertyKey idealStatePropertyKey = _keyBuilder.idealStates(tableName);

    // Set all partitions to offline to unload them from the servers
    boolean updateSuccessful;
    do {
      final IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
      final Set<String> instanceSet = idealState.getInstanceSet(segmentName);
      if (instanceSet == null || instanceSet.size() == 0) {
        // We are trying to refresh a segment, but there are no instances currently assigned for fielding this segment.
        // When those instances do come up, the segment will be uploaded correctly, so return success but log a warning.
        LOGGER.warn("No instances as yet for segment {}, table {}", segmentName, tableName);
        return true;
      }
      for (final String instance : instanceSet) {
        idealState.setPartitionState(segmentName, instance, "OFFLINE");
      }
      updateSuccessful = helixDataAccessor.updateProperty(idealStatePropertyKey, idealState);
    } while (!updateSuccessful);

    // Check that the ideal state has been written to ZK
    IdealState updatedIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    Map<String, String> instanceStateMap = updatedIdealState.getInstanceStateMap(segmentName);
    for (String state : instanceStateMap.values()) {
      if (!"OFFLINE".equals(state)) {
        LOGGER.error("Failed to write OFFLINE ideal state!");
        return false;
      }
    }

    // Wait until the partitions are offline in the external view
    LOGGER.info("Wait until segment - " + segmentName + " to be OFFLINE in ExternalView");
    if (!ifExternalViewChangeReflectedForState(tableName, segmentName, "OFFLINE",
        _externalViewOnlineToOfflineTimeoutMillis, false)) {
      LOGGER.error(
          "External view for segment {} did not reflect the ideal state of OFFLINE within the {} ms time limit",
          segmentName, _externalViewOnlineToOfflineTimeoutMillis);
      return false;
    }

    // Set all partitions to online so that they load the new segment data
    do {
      final IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
      final Set<String> instanceSet = idealState.getInstanceSet(segmentName);
      LOGGER.info("Found {} instances for segment '{}', in ideal state", instanceSet.size(), segmentName);
      for (final String instance : instanceSet) {
        idealState.setPartitionState(segmentName, instance, "ONLINE");
        LOGGER.info("Setting Ideal State for segment '{}' to ONLINE for instance '{}'", segmentName, instance);
      }
      updateSuccessful = helixDataAccessor.updateProperty(idealStatePropertyKey, idealState);
    } while (!updateSuccessful);

    // Check that the ideal state has been written to ZK
    updatedIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    instanceStateMap = updatedIdealState.getInstanceStateMap(segmentName);
    LOGGER.info("Found {} instances for segment '{}', after updating ideal state", instanceStateMap.size(), segmentName);
    for (String state : instanceStateMap.values()) {
      if (!"ONLINE".equals(state)) {
        LOGGER.error("Failed to write ONLINE ideal state!");
        return false;
      }
    }

    LOGGER.info("Refresh is done for segment - " + segmentName);
    return true;
  }

  /*
   *  fetch list of segments assigned to a give table from ideal state
   */
  public List<String> getAllSegmentsForResource(String tableName) {
    List<String> segmentsInResource = new ArrayList<String>();
    switch (TableNameBuilder.getTableTypeFromTableName(tableName)) {
      case REALTIME:
        for (RealtimeSegmentZKMetadata segmentZKMetadata : ZKMetadataProvider.getRealtimeSegmentZKMetadataListForTable(
            getPropertyStore(), tableName)) {
          segmentsInResource.add(segmentZKMetadata.getSegmentName());
        }

        break;
      case OFFLINE:
        for (OfflineSegmentZKMetadata segmentZKMetadata : ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(
            getPropertyStore(), tableName)) {
          segmentsInResource.add(segmentZKMetadata.getSegmentName());
        }
        break;
      default:
        break;
    }
    return segmentsInResource;
  }

  public Map<String, List<String>> getInstanceToSegmentsInATableMap(String tableName) {
    Map<String, List<String>> instancesToSegmentsMap = new HashMap<String, List<String>>();
    IdealState is = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    Set<String> segments = is.getPartitionSet();

    for (String segment : segments) {
      Set<String> instances = is.getInstanceSet(segment);
      for (String instance : instances) {
        if (instancesToSegmentsMap.containsKey(instance)) {
          instancesToSegmentsMap.get(instance).add(segment);
        } else {
          List<String> a = new ArrayList<String>();
          a.add(segment);
          instancesToSegmentsMap.put(instance, a);
        }
      }
    }

    return instancesToSegmentsMap;
  }

  /**
   * Toggle the status of segment between ONLINE (enable = true) and OFFLINE (enable = FALSE).
   *
   * @param tableName: Name of table to which the segment belongs.
   * @param segments: List of segment for which to toggle the status.
   * @param enable: True for ONLINE, False for OFFLINE.
   * @param timeoutInSeconds Time out for toggling segment state.
   * @return
   */
  public PinotResourceManagerResponse toggleSegmentState(String tableName, List<String> segments, boolean enable,
      long timeoutInSeconds) {
    String status = (enable) ? "ONLINE" : "OFFLINE";

    HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    PropertyKey idealStatePropertyKey = _keyBuilder.idealStates(tableName);

    boolean updateSuccessful;
    boolean externalViewUpdateSuccessful = true;
    long deadline = System.currentTimeMillis() + 1000 * timeoutInSeconds;

    // Set all partitions to offline to unload them from the servers
    do {
      final IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);

      for (String segmentName : segments) {
        final Set<String> instanceSet = idealState.getInstanceSet(segmentName);
        if (instanceSet == null || instanceSet.isEmpty()) {
          return new PinotResourceManagerResponse("Segment " + segmentName + " not found.", false);
        }
        for (final String instance : instanceSet) {
          idealState.setPartitionState(segmentName, instance, status);
        }
      }
      updateSuccessful = helixDataAccessor.updateProperty(idealStatePropertyKey, idealState);
    } while (!updateSuccessful && (System.currentTimeMillis() <= deadline));

    // Check that the ideal state has been updated.
    LOGGER.info("Ideal state successfully updated, waiting to update external view");
    IdealState updatedIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    for (String segmentName : segments) {
      Map<String, String> instanceStateMap = updatedIdealState.getInstanceStateMap(segmentName);
      for (String state : instanceStateMap.values()) {
        if (!status.equals(state)) {
          return new PinotResourceManagerResponse("Error: Failed to update Ideal state when setting status " +
              status + " for segment " + segmentName, false);
        }
      }

      // Wait until the partitions are offline in the external view
      if (!ifExternalViewChangeReflectedForState(tableName, segmentName, status, (timeoutInSeconds * 1000),
          true)) {
        externalViewUpdateSuccessful = false;
      }
    }

    return (externalViewUpdateSuccessful) ? new PinotResourceManagerResponse(("Success: Segment(s) " + " now " + status), true) :
        new PinotResourceManagerResponse("Error: Timed out. External view not completely updated", false);
  }

  public boolean hasRealtimeTable(String tableName) {
    String actualTableName = tableName + "_REALTIME";
    return getAllPinotTableNames().contains(actualTableName);
  }

  public boolean hasOfflineTable(String tableName) {
    String actualTableName = tableName + "_OFFLINE";
    return getAllPinotTableNames().contains(actualTableName);
  }

  public AbstractTableConfig getOfflineTableConfig(String offlineTableName) throws JsonParseException,
      JsonMappingException, JsonProcessingException, JSONException, IOException {
    return ZKMetadataProvider.getOfflineTableConfig(getPropertyStore(), offlineTableName);
  }

  public AbstractTableConfig getRealtimeTableConfig(String realtimeTableName) throws JsonParseException,
      JsonMappingException, JsonProcessingException, JSONException, IOException {
    return ZKMetadataProvider.getRealtimeTableConfig(getPropertyStore(), realtimeTableName);
  }

  public AbstractTableConfig getTableConfig(String tableName, TableType type) throws JsonParseException,
      JsonMappingException, JsonProcessingException, JSONException, IOException {
    String actualTableName = new TableNameBuilder(type).forTable(tableName);
    AbstractTableConfig config = null;
    if (type == TableType.REALTIME) {
      config = ZKMetadataProvider.getRealtimeTableConfig(getPropertyStore(), actualTableName);
    } else {
      config = ZKMetadataProvider.getOfflineTableConfig(getPropertyStore(), actualTableName);
    }
    return config;
  }

  public List<String> getServerInstancesForTable(String tableName, TableType type) throws JsonParseException,
      JsonMappingException, JsonProcessingException, JSONException, IOException {
    String actualTableName = new TableNameBuilder(type).forTable(tableName);
    AbstractTableConfig config = null;
    if (type == TableType.REALTIME) {
      config = ZKMetadataProvider.getRealtimeTableConfig(getPropertyStore(), actualTableName);
    } else {
      config = ZKMetadataProvider.getOfflineTableConfig(getPropertyStore(), actualTableName);
    }
    String serverTenantName =
        ControllerTenantNameBuilder.getTenantName(config.getTenantConfig().getServer(), type.getServerType());
    List<String> serverInstances = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, serverTenantName);
    return serverInstances;
  }

  public List<String> getBrokerInstancesForTable(String tableName, TableType type) throws JsonParseException,
      JsonMappingException, JsonProcessingException, JSONException, IOException {
    String actualTableName = new TableNameBuilder(type).forTable(tableName);
    AbstractTableConfig config = null;
    if (type == TableType.REALTIME) {
      config = ZKMetadataProvider.getRealtimeTableConfig(getPropertyStore(), actualTableName);
    } else {
      config = ZKMetadataProvider.getOfflineTableConfig(getPropertyStore(), actualTableName);
    }
    String brokerTenantName =
        ControllerTenantNameBuilder.getBrokerTenantNameForTenant(config.getTenantConfig().getBroker());
    List<String> serverInstances = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTenantName);
    return serverInstances;
  }

  public PinotResourceManagerResponse enableInstance(String instanceName) {
    return toogleInstance(instanceName, true, 10);
  }

  public PinotResourceManagerResponse disableInstance(String instanceName) {
    return toogleInstance(instanceName, false, 10);
  }

  /**
   * Drop the instance from helix cluster. Instance will not be dropped if:
   * - It is a live instance.
   * - Has at least one ONLINE segment.
   *
   * @param instanceName: Name of the instance to be dropped.
   * @return
   */
  public PinotResourceManagerResponse dropInstance(String instanceName) {
    if (!instanceExists(instanceName)) {
      return new PinotResourceManagerResponse("Instance " + instanceName + " does not exist.", false);
    }

    HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    LiveInstance liveInstance = helixDataAccessor.getProperty(_keyBuilder.liveInstance(instanceName));

    if (liveInstance != null) {
      PropertyKey currentStatesKey =
          _keyBuilder.currentStates(instanceName, liveInstance.getSessionId());
      List<CurrentState> currentStates = _helixDataAccessor.getChildValues(currentStatesKey);

      if (currentStates != null) {
        for (CurrentState currentState : currentStates) {
          for (String state : currentState.getPartitionStateMap().values()) {
            if (state.equalsIgnoreCase(SegmentOnlineOfflineStateModel.ONLINE)) {
              return new PinotResourceManagerResponse(("Instance " + instanceName + " has online partitions"), false);
            }
          }
        }
      } else {
        return new PinotResourceManagerResponse("Cannot drop live instance " + instanceName
            + " please stop the instance first.", false);
      }
    }

    // Disable the instance first.
    toogleInstance(instanceName, false, 10);
    _helixAdmin.dropInstance(_helixClusterName, getHelixInstanceConfig(instanceName));

    return new PinotResourceManagerResponse("Instance " + instanceName + " dropped.", true);
  }

  /**
   * Toggle the status of an Instance between OFFLINE and ONLINE.
   * Keeps checking until ideal-state is successfully updated or times out.
   *
   * @param instanceName: Name of Instance for which the status needs to be toggled.
   * @param toggle: 'True' for ONLINE 'False' for OFFLINE.
   * @param timeOutInSeconds: Time-out for setting ideal-state.
   * @return
   */
  public PinotResourceManagerResponse toogleInstance(String instanceName, boolean toggle, int timeOutInSeconds) {
    if (!instanceExists(instanceName)) {
      return new PinotResourceManagerResponse("Instance " + instanceName + " does not exist.", false);
    }

    _helixAdmin.enableInstance(_helixClusterName, instanceName, toggle);
    long deadline = System.currentTimeMillis() + 1000 * timeOutInSeconds;
    boolean toggleSucceed = false;
    String beforeToggleStates =
        (toggle) ? SegmentOnlineOfflineStateModel.OFFLINE : SegmentOnlineOfflineStateModel.ONLINE;

    while (System.currentTimeMillis() < deadline) {
      toggleSucceed = true;
      PropertyKey liveInstanceKey = _keyBuilder.liveInstance(instanceName);
      LiveInstance liveInstance = _helixDataAccessor.getProperty(liveInstanceKey);
      if (liveInstance == null) {
        if (toggle) {
          return PinotResourceManagerResponse.FAILURE_RESPONSE;
        } else {
          return PinotResourceManagerResponse.SUCCESS_RESPONSE;
        }
      }
      PropertyKey instanceCurrentStatesKey =
          _keyBuilder.currentStates(instanceName, liveInstance.getSessionId());
      List<CurrentState> instanceCurrentStates = _helixDataAccessor.getChildValues(instanceCurrentStatesKey);
      if (instanceCurrentStates == null) {
        return PinotResourceManagerResponse.SUCCESS_RESPONSE;
      } else {
        for (CurrentState currentState : instanceCurrentStates) {
          for (String state : currentState.getPartitionStateMap().values()) {
            if (beforeToggleStates.equals(state)) {
              toggleSucceed = false;
            }
          }
        }
      }
      if (toggleSucceed) {
        return (toggle) ? new PinotResourceManagerResponse("Instance " + instanceName + " enabled.", true)
            : new PinotResourceManagerResponse("Instance " + instanceName + " disabled.", true);
      } else {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
        }
      }
    }
    return new PinotResourceManagerResponse("Instance enable/disable failed, timeout.", false);
  }

  /**
   * Check if an Instance exists in the Helix cluster.
   *
   * @param instanceName: Name of instance to check.
   * @return True if instance exists in the Helix cluster, False otherwise.
   */
  public boolean instanceExists(String instanceName) {
    HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    InstanceConfig config = helixDataAccessor.getProperty(_keyBuilder.instanceConfig(instanceName));
    return (config != null);
  }

  public boolean isSingleTenantCluster() {
    return _isSingleTenantCluster;
  }

  /**
   * Computes the broker nodes that are untagged and free to be used.
   * @return List of online untagged broker instances.
   */
  public List<String> getOnlineUnTaggedBrokerInstanceList() {

    final List<String> instanceList =
            _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    final List<String> liveInstances = _helixDataAccessor.getChildNames(_keyBuilder.liveInstances());
    instanceList.retainAll(liveInstances);
    return instanceList;
  }

  /**
   * Computes the server nodes that are untagged and free to be used.
   * @return List of untagged online server instances.
   */
  public List<String> getOnlineUnTaggedServerInstanceList() {
    final List<String> instanceList =
            _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    final List<String> liveInstances = _helixDataAccessor.getChildNames(_keyBuilder.liveInstances());
    instanceList.retainAll(liveInstances);
    return instanceList;
  }

  public List<String> getOnlineInstanceList() {
    return _helixDataAccessor.getChildNames(_keyBuilder.liveInstances());
  }

  /**
   * Provides admin endpoints for the provided data instances
   * @param instances instances for which to read endpoints
   * @return returns map of instances to their admin endpoints.
   * The return value is a bimap because admin instances are typically used for
   * http requests. So, on response, we need mapping from the endpoint to the
   * server instances. With BiMap, both mappings are easily available
   */
  public @Nonnull
  BiMap<String, String> getDataInstanceAdminEndpoints(@Nonnull Set<String> instances) {
    Preconditions.checkNotNull(instances);
    BiMap<String, String> endpointToInstance = HashBiMap.create(instances.size());
    for (String instance : instances) {
      InstanceConfig helixInstanceConfig = getHelixInstanceConfig(instance);
      ZNRecord record = helixInstanceConfig.getRecord();
      String[] hostnameSplit = helixInstanceConfig.getHostName().split("_");
      Preconditions.checkState(hostnameSplit.length >= 2);
      String port = record.getSimpleField(CommonConstants.Helix.Instance.ADMIN_PORT_KEY);
      endpointToInstance.put(instance, hostnameSplit[1] + ":" + port);
    }
    return endpointToInstance;
  }
}
