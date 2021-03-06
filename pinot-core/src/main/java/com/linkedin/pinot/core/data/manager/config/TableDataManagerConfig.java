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
package com.linkedin.pinot.core.data.manager.config;

import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;


/**
 * The config used for TableDataManager.
 */
public class TableDataManagerConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableDataManagerConfig.class);

  private static final String TABLE_DATA_MANAGER_TYPE = "dataManagerType";
  private static final String READ_MODE = "readMode";
  private static final String TABLE_DATA_MANAGER_DATA_DIRECTORY = "directory";
  private static final String TABLE_DATA_MANAGER_NAME = "name";
  private static final String REALTIME_AVG_MULTI_VALUE_COUNT = "avgMvCount";
  private static final int DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT = 2;

  private final Configuration _tableDataManagerConfig;

  public TableDataManagerConfig(Configuration tableDataManagerConfig)
      throws ConfigurationException {
    _tableDataManagerConfig = tableDataManagerConfig;
  }

  public String getReadMode() {
    return _tableDataManagerConfig.getString(READ_MODE);
  }

  public Configuration getConfig() {
    return _tableDataManagerConfig;
  }

  public String getTableDataManagerType() {
    return _tableDataManagerConfig.getString(TABLE_DATA_MANAGER_TYPE);
  }

  public String getDataDir() {
    return _tableDataManagerConfig.getString(TABLE_DATA_MANAGER_DATA_DIRECTORY);
  }

  public String getTableName() {
    return _tableDataManagerConfig.getString(TABLE_DATA_MANAGER_NAME);
  }

  public int getRealtimeAvgMvCount() {
    return _tableDataManagerConfig.getInt(REALTIME_AVG_MULTI_VALUE_COUNT);
  }

  public static TableDataManagerConfig getDefaultHelixTableDataManagerConfig(
      InstanceDataManagerConfig instanceDataManagerConfig, String tableName)
      throws ConfigurationException {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    assert tableType != null;

    Configuration defaultConfig = new PropertiesConfiguration();
    defaultConfig.addProperty(TABLE_DATA_MANAGER_NAME, tableName);
    String dataDir = instanceDataManagerConfig.getInstanceDataDir() + "/" + tableName;
    defaultConfig.addProperty(TABLE_DATA_MANAGER_DATA_DIRECTORY, dataDir);
    defaultConfig.addProperty(IndexLoadingConfigMetadata.KEY_OF_COLUMN_MIN_MAX_VALUE_GENERATOR_MODE,
        ColumnMinMaxValueGeneratorMode.TIME.toString());
    if (instanceDataManagerConfig.getReadMode() != null) {
      defaultConfig.addProperty(READ_MODE, instanceDataManagerConfig.getReadMode().toString());
    } else {
      defaultConfig.addProperty(READ_MODE, ReadMode.heap);
    }
    int avgMultiValueCount = DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT;
    if (instanceDataManagerConfig.getAvgMultiValueCount() != null) {
      try {
        avgMultiValueCount = Integer.valueOf(instanceDataManagerConfig.getAvgMultiValueCount());
      } catch (NumberFormatException e) {
        // Ignore
      }
    }
    defaultConfig.addProperty(REALTIME_AVG_MULTI_VALUE_COUNT, new Integer(avgMultiValueCount));

    if (instanceDataManagerConfig.getSegmentFormatVersion() != null) {
      defaultConfig.addProperty(IndexLoadingConfigMetadata.KEY_OF_SEGMENT_FORMAT_VERSION,
          instanceDataManagerConfig.getSegmentFormatVersion());
    }
    if (instanceDataManagerConfig.isEnableDefaultColumns()) {
      defaultConfig.addProperty(IndexLoadingConfigMetadata.KEY_OF_ENABLE_DEFAULT_COLUMNS, true);
    }
    TableDataManagerConfig tableDataManagerConfig = new TableDataManagerConfig(defaultConfig);

    switch (tableType) {
      case OFFLINE:
        defaultConfig.addProperty(TABLE_DATA_MANAGER_TYPE, "offline");
        break;
      case REALTIME:
        defaultConfig.addProperty(TABLE_DATA_MANAGER_TYPE, "realtime");
        break;

      default:
        throw new UnsupportedOperationException("Not supported table type for - " + tableName);
    }
    return tableDataManagerConfig;
  }

  public void overrideConfigs(String tableName, AbstractTableConfig tableConfig) {
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    _tableDataManagerConfig.setProperty(READ_MODE, indexingConfig.getLoadMode().toLowerCase());
    _tableDataManagerConfig.setProperty(TABLE_DATA_MANAGER_NAME, tableConfig.getTableName());

    List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    if (invertedIndexColumns != null) {
      _tableDataManagerConfig.setProperty(IndexLoadingConfigMetadata.KEY_OF_LOADING_INVERTED_INDEX,
          invertedIndexColumns);
    }
    String starTreeFormat = indexingConfig.getStarTreeFormat();
    if (starTreeFormat != null) {
      _tableDataManagerConfig.setProperty(IndexLoadingConfigMetadata.KEY_OF_STAR_TREE_FORMAT_VERSION, starTreeFormat);
    }
    String columnMinMaxValueGeneratorMode = indexingConfig.getColumnMinMaxValueGeneratorMode();
    if (columnMinMaxValueGeneratorMode != null) {
      _tableDataManagerConfig.setProperty(IndexLoadingConfigMetadata.KEY_OF_COLUMN_MIN_MAX_VALUE_GENERATOR_MODE,
          columnMinMaxValueGeneratorMode);
    }

    // Server configuration is always to DEFAULT or configured value
    // Apply table configuration only if the server configuration is set with table config
    // overriding server config
    //
    // Server config not set means table config has no impact. This provides additional
    // security from inadvertent changes as both config will need to be enabled for the
    // change to take effect
    SegmentVersion tableConfigVersion =
        SegmentVersion.fromString(indexingConfig.getSegmentFormatVersion(), SegmentVersion.DEFAULT_TABLE_VERSION);

    SegmentVersion serverConfigVersion = SegmentVersion.fromString(
        _tableDataManagerConfig.getString(IndexLoadingConfigMetadata.KEY_OF_SEGMENT_FORMAT_VERSION),
        SegmentVersion.DEFAULT_SERVER_VERSION);

    // override server based configuration with table level configuration iff table configuration
    // is less than server configuration
    if (SegmentVersion.compare(tableConfigVersion, serverConfigVersion) < 0) {
      LOGGER.info("Overriding server segment format version: {} with table version: {} for table: {}",
          serverConfigVersion, tableConfigVersion, tableName);
      _tableDataManagerConfig.setProperty(IndexLoadingConfigMetadata.KEY_OF_SEGMENT_FORMAT_VERSION,
          indexingConfig.getSegmentFormatVersion());
    } else {
      LOGGER.info("Loading table: {} with server configured segment format version: {}", tableName,
          serverConfigVersion);
    }
  }

  public IndexLoadingConfigMetadata getIndexLoadingConfigMetadata() {
    return new IndexLoadingConfigMetadata(_tableDataManagerConfig);
  }
}
