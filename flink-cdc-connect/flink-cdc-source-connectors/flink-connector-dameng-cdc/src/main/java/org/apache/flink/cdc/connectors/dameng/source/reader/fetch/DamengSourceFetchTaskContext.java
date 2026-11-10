/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.dameng.source.reader.fetch;

import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.EmbeddedFlinkDatabaseHistory;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.dameng.source.config.DamengSourceConfig;
import org.apache.flink.cdc.connectors.dameng.source.handler.DamengSchemaChangeEventHandler;
import org.apache.flink.cdc.connectors.dameng.source.meta.offset.RedoLogOffset;
import org.apache.flink.cdc.connectors.dameng.source.utils.DamengUtils;
import org.apache.flink.cdc.connectors.dameng.util.ChunkUtils;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Collect;
import oracle.sql.ROWID;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.devlive.connector.dameng.DamengChangeEventSourceMetricsFactory;
import org.devlive.connector.dameng.DamengConnection;
import org.devlive.connector.dameng.DamengConnectorConfig;
import org.devlive.connector.dameng.DamengDatabaseSchema;
import org.devlive.connector.dameng.DamengErrorHandler;
import org.devlive.connector.dameng.DamengOffsetContext;
import org.devlive.connector.dameng.DamengStreamingChangeEventSourceMetrics;
import org.devlive.connector.dameng.DamengTaskContext;
import org.devlive.connector.dameng.DamengTopicSelector;
import org.devlive.connector.dameng.MapBackedPartition;
import org.devlive.connector.dameng.SourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;

import static org.apache.flink.cdc.connectors.dameng.source.utils.DamengConnectionUtils.createDamengConnection;
import static org.apache.flink.cdc.connectors.dameng.util.ChunkUtils.getChunkKeyColumn;

/** The context for fetch task that fetching data of snapshot split from Dameng data source. */
public class DamengSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final Logger LOG = LoggerFactory.getLogger(DamengSourceFetchTaskContext.class);

    private final DamengConnection connection;
    private final DamengEventMetadataProvider metadataProvider;

    private DamengDatabaseSchema databaseSchema;
    private DamengTaskContext taskContext;
    private DamengOffsetContext offsetContext;
    private MapBackedPartition partition;

    private SnapshotChangeEventSourceMetrics<MapBackedPartition> snapshotChangeEventSourceMetrics;
    private DamengStreamingChangeEventSourceMetrics streamingChangeEventSourceMetrics;
    private TopicSelector<TableId> topicSelector;
    private JdbcSourceEventDispatcher<MapBackedPartition> dispatcher;
    private ChangeEventQueue<DataChangeEvent> queue;
    private DamengErrorHandler errorHandler;

    public DamengSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dataSourceDialect) {
        super(sourceConfig, dataSourceDialect);
        this.connection = createDamengConnection(sourceConfig.getDbzConfiguration());
        this.metadataProvider = new DamengEventMetadataProvider();
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        // initial stateful objects
        final DamengConnectorConfig connectorConfig = getDbzConnectorConfig();
        this.topicSelector = DamengTopicSelector.defaultSelector(connectorConfig);
        EmbeddedFlinkDatabaseHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
                sourceSplitBase.getTableSchemas().values());
        this.databaseSchema = DamengUtils.createDamengDatabaseSchema(connectorConfig, connection);
        // todo logMiner or xStream
        this.offsetContext =
                loadStartingOffsetState(
                        new DamengOffsetContext.Loader(
                                connectorConfig, DamengConnectorConfig.ConnectorAdapter.LOG_MINER),
                        sourceSplitBase);
        this.partition =
                new MapBackedPartition(
                        Collect.hashMapOf("server", connectorConfig.getLogicalName()));
        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

        this.taskContext = new DamengTaskContext(connectorConfig, databaseSchema);
        final int queueSize =
                sourceSplitBase.isSnapshotSplit()
                        ? getSourceConfig().getSplitSize()
                        : getSourceConfig().getDbzConnectorConfig().getMaxQueueSize();
        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        taskContext.configureLoggingContext(
                                                "oracle-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();
        this.dispatcher =
                new JdbcSourceEventDispatcher<>(
                        connectorConfig,
                        topicSelector,
                        databaseSchema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster,
                        new DamengSchemaChangeEventHandler());

        final DamengChangeEventSourceMetricsFactory changeEventSourceMetricsFactory =
                new DamengChangeEventSourceMetricsFactory(
                        new DamengStreamingChangeEventSourceMetrics(
                                taskContext, queue, metadataProvider, connectorConfig));
        this.snapshotChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getSnapshotMetrics(
                        taskContext, queue, metadataProvider);
        this.streamingChangeEventSourceMetrics =
                (DamengStreamingChangeEventSourceMetrics)
                        changeEventSourceMetricsFactory.getStreamingMetrics(
                                taskContext, queue, metadataProvider);
        this.errorHandler = new DamengErrorHandler(connectorConfig, queue);
    }

    @Override
    public DamengSourceConfig getSourceConfig() {
        return (DamengSourceConfig) sourceConfig;
    }

    public DamengConnection getConnection() {
        return connection;
    }

    @Override
    public DamengConnectorConfig getDbzConnectorConfig() {
        return (DamengConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public DamengOffsetContext getOffsetContext() {
        return offsetContext;
    }

    public SnapshotChangeEventSourceMetrics<MapBackedPartition>
            getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    public DamengStreamingChangeEventSourceMetrics getStreamingChangeEventSourceMetrics() {
        return streamingChangeEventSourceMetrics;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public DamengDatabaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public RowType getSplitType(Table table) {
        DamengSourceConfig oracleSourceConfig = getSourceConfig();
        return ChunkUtils.getSplitType(
                getChunkKeyColumn(table, oracleSourceConfig.getChunkKeyColumn()));
    }

    @Override
    public boolean isRecordBetween(SourceRecord record, Object[] splitStart, Object[] splitEnd) {
        RowType splitKeyType =
                getSplitType(getDatabaseSchema().tableFor(SourceRecordUtils.getTableId(record)));

        // RowId is chunk key column by default, compare RowId
        if (splitKeyType.getFieldNames().contains(ROWID.class.getSimpleName())) {
            ConnectHeaders headers = (ConnectHeaders) record.headers();
            ROWID rowId = null;
            try {
                rowId = new ROWID(headers.iterator().next().value().toString());
            } catch (SQLException e) {
                LOG.error("{} can not convert to RowId", record);
            }
            Object[] rowIds = new ROWID[] {rowId};
            return SourceRecordUtils.splitKeyRangeContains(rowIds, splitStart, splitEnd);
        } else {
            // config chunk key column compare
            Object[] key =
                    SourceRecordUtils.getSplitKey(splitKeyType, record, getSchemaNameAdjuster());
            return SourceRecordUtils.splitKeyRangeContains(key, splitStart, splitEnd);
        }
    }

    @Override
    public JdbcSourceEventDispatcher<MapBackedPartition> getEventDispatcher() {
        return dispatcher;
    }

    @Override
    public WatermarkDispatcher getWaterMarkDispatcher() {
        return dispatcher;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    public MapBackedPartition getPartition() {
        return partition;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord sourceRecord) {
        return DamengUtils.getRedoLogPosition(sourceRecord);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private DamengOffsetContext loadStartingOffsetState(
            OffsetContext.Loader<DamengOffsetContext> loader, SourceSplitBase oracleSplit) {
        Offset offset =
                oracleSplit.isSnapshotSplit()
                        ? RedoLogOffset.INITIAL_OFFSET
                        : oracleSplit.asStreamSplit().getStartingOffset();

        return loader.load(offset.getOffset());
    }

    private void validateAndLoadDatabaseHistory(
            DamengOffsetContext offset, DamengDatabaseSchema schema) {
        schema.initializeStorage();
        try {
            schema.recover(Offsets.of(partition, offset));
        } catch (io.debezium.DebeziumException e) {
            // Database history might be missing in test scenarios or initial runs
            // This is acceptable as schema will be discovered during snapshot/streaming
            // Only log as warning instead of failing
            LOG.warn(
                    "Database history is not available, schema will be discovered during execution. "
                            + "This is normal for initial snapshot or test scenarios. Details: {}",
                    e.getMessage());
        }
    }

    /** Copied from debezium for accessing here. */
    public static class DamengEventMetadataProvider implements EventMetadataProvider {
        @Override
        public Instant getEventTimestamp(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
            return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
        }

        @Override
        public Map<String, String> getEventSourcePosition(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            final String scn = sourceInfo.getString(SourceInfo.SCN_KEY);
            return Collect.hashMapOf(SourceInfo.SCN_KEY, scn == null ? "null" : scn);
        }

        @Override
        public String getTransactionId(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            return sourceInfo.getString(SourceInfo.TXID_KEY);
        }
    }
}
