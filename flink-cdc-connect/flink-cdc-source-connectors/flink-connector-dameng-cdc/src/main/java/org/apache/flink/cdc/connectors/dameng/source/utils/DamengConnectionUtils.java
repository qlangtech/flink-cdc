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

package org.apache.flink.cdc.connectors.dameng.source.utils;

import org.apache.flink.cdc.connectors.dameng.source.meta.offset.RedoLogOffset;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.devlive.connector.dameng.Scn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

/** Dameng connection Utilities. */
public class DamengConnectionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DamengConnectionUtils.class);

    /** Returned by column metadata in Dameng if no scale is set. */
    private static final int DAMENG_UNSET_SCALE = -127;

    /** show current scn sql in dameng. have move to */
    // public static final String SHOW_CURRENT_SCN = "SELECT CLSN FROM V$ARCH_FILE WHERE STATUS =
    // 'ACTIVE'";

    /**
     * Creates a new {@link org.devlive.connector.dameng.DamengConnection}, but not open the
     * connection.
     */
    public static org.devlive.connector.dameng.DamengConnection createDamengConnection(
            Configuration configuration) {
        return createDamengConnection(JdbcConfiguration.adapt(configuration));
    }

    /**
     * Creates a new {@link org.devlive.connector.dameng.DamengConnection}, but not open the
     * connection.
     */
    public static org.devlive.connector.dameng.DamengConnection createDamengConnection(
            JdbcConfiguration dbzConfiguration) {
        Configuration configuration = dbzConfiguration.subset(DATABASE_CONFIG_PREFIX, true);
        return new org.devlive.connector.dameng.DamengConnection(
                configuration.isEmpty() ? dbzConfiguration : JdbcConfiguration.adapt(configuration),
                DamengConnectionUtils.class::getClassLoader);
    }

    /** Fetch current redoLog offsets in Dameng Server. DamengConnection.SHOW_CURRENT_SCN */
    public static RedoLogOffset currentRedoLogOffset(
            String queryCurrentRedoLogOffsetSQL, JdbcConnection jdbc) {
        try {
            return jdbc.queryAndMap(
                    queryCurrentRedoLogOffsetSQL,
                    rs -> {
                        if (rs.next()) {
                            final String scn = rs.getString(1);
                            return new RedoLogOffset(Scn.valueOf(scn).longValue());
                        } else {
                            throw new FlinkRuntimeException(
                                    "Cannot read the scn via '"
                                            + queryCurrentRedoLogOffsetSQL
                                            + "'. Make sure your server is correctly configured");
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "Cannot read the redo log position via '"
                            + queryCurrentRedoLogOffsetSQL
                            + "'. Make sure your server is correctly configured",
                    e);
        }
    }

    public static List<TableId> listTables(
            JdbcConnection jdbcConnection, RelationalTableFilters tableFilters)
            throws SQLException {
        final List<TableId> capturedTableIds = new ArrayList<>();

        Set<TableId> tableIdSet = new HashSet<>();
        // Query for Dameng database tables, excluding system tables
        String queryTablesSql =
                "SELECT OWNER, TABLE_NAME, TABLESPACE_NAME FROM ALL_TABLES \n"
                        + "WHERE TABLESPACE_NAME IS NOT NULL "
                        + "AND TABLESPACE_NAME NOT IN ('SYSTEM', 'SYSAUX', 'MAIN', 'ROLL') "
                        + "AND OWNER NOT IN ('SYS', 'SYSDBA', 'SYSSSO', 'SYSAUDITOR')";
        try {
            jdbcConnection.query(
                    queryTablesSql,
                    rs -> {
                        while (rs.next()) {
                            String schemaName = rs.getString(1);
                            String tableName = rs.getString(2);
                            TableId tableId =
                                    new TableId(jdbcConnection.database(), schemaName, tableName);
                            tableIdSet.add(tableId);
                        }
                    });
        } catch (SQLException e) {
            LOG.warn("SQL execute error, sql:{}", queryTablesSql, e);
        }

        for (TableId tableId : tableIdSet) {
            if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                capturedTableIds.add(tableId);
                LOG.info("\t including '{}' for further processing", tableId);
            } else {
                LOG.debug("\t '{}' is filtered out of capturing", tableId);
            }
        }

        return capturedTableIds;
    }
}
