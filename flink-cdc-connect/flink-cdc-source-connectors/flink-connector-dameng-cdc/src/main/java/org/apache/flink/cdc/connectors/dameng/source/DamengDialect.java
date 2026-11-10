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

package org.apache.flink.cdc.connectors.dameng.source;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.dameng.source.assigner.splitter.DamengChunkSplitter;
import org.apache.flink.cdc.connectors.dameng.source.config.DamengSourceConfig;
import org.apache.flink.cdc.connectors.dameng.source.reader.fetch.DamengScanFetchTask;
import org.apache.flink.cdc.connectors.dameng.source.reader.fetch.DamengSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.dameng.source.reader.fetch.DamengStreamFetchTask;
import org.apache.flink.cdc.connectors.dameng.source.utils.DamengConnectionUtils;
import org.apache.flink.cdc.connectors.dameng.source.utils.DamengSchema;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;
import org.devlive.connector.dameng.DamengConnection;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.dameng.source.utils.DamengConnectionUtils.createDamengConnection;
import static org.apache.flink.cdc.connectors.dameng.source.utils.DamengConnectionUtils.currentRedoLogOffset;

/** The {@link JdbcDataSourceDialect} implementation for Dameng datasource. */
@Experimental
public class DamengDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private transient DamengSchema damengSchema;
    /**
     * Get current SCN from Dameng database. Dameng database provides
     * DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER() function to get current SCN.
     */
    public static final String SHOW_CURRENT_SCN =
            "SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER() AS CURRENT_SCN FROM DUAL";

    private transient Tables.TableFilter filters;

    @Override
    public String getName() {
        return "Dameng";
    }

    @Override
    public final Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return currentRedoLogOffset(
                    getQueryCurrentRedoLogOffsetSQLShowCurrentScn(), jdbcConnection);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Read the redoLog offset error", e);
        }
    }

    protected String getQueryCurrentRedoLogOffsetSQLShowCurrentScn() {
        return SHOW_CURRENT_SCN;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            DamengConnection damengConnection = (DamengConnection) jdbcConnection;
            return damengConnection.getOracleVersion().getMajor() == 7;
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error reading dameng variables: " + e.getMessage(), e);
        }
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        return DamengConnectionUtils.createDamengConnection(
                sourceConfig.getDbzConnectorConfig().getJdbcConfig());
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new DamengChunkSplitter(
                sourceConfig, this, ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
    }

    @Override
    public ChunkSplitter createChunkSplitter(
            JdbcSourceConfig sourceConfig, ChunkSplitterState chunkSplitterState) {
        return new DamengChunkSplitter(sourceConfig, this, chunkSplitterState);
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new DamengPooledDataSourceFactory();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        DamengSourceConfig damengSourceConfig = (DamengSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return DamengConnectionUtils.listTables(
                    jdbcConnection, damengSourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

        try (DamengConnection jdbc = createDamengConnection(sourceConfig.getDbzConfiguration())) {
            // fetch table schemas
            Map<TableId, TableChange> tableSchemas = new HashMap<>();
            for (TableId tableId : capturedTableIds) {
                TableChange tableSchema = queryTableSchema(jdbc, tableId);
                tableSchemas.put(tableId, tableSchema);
            }
            return tableSchemas;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error to discover table schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (damengSchema == null) {
            damengSchema = new DamengSchema();
        }
        return damengSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public DamengSourceFetchTaskContext createFetchTaskContext(JdbcSourceConfig taskSourceConfig) {
        return new DamengSourceFetchTaskContext(taskSourceConfig, this);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new DamengScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new DamengStreamFetchTask(sourceSplitBase.asStreamSplit());
        }
    }

    @Override
    public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
        if (filters == null) {
            this.filters = sourceConfig.getTableFilters().dataCollectionFilter();
        }

        return filters.isIncluded(tableId);
    }
}
