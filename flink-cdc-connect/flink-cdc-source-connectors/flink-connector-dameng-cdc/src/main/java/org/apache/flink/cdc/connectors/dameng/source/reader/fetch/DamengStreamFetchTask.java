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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;

import io.debezium.config.Configuration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.devlive.connector.dameng.DamengConnection;
import org.devlive.connector.dameng.DamengConnectorConfig;
import org.devlive.connector.dameng.DamengDatabaseSchema;
import org.devlive.connector.dameng.DamengOffsetContext;
import org.devlive.connector.dameng.DamengStreamingChangeEventSourceMetrics;
import org.devlive.connector.dameng.DamengTaskContext;
import org.devlive.connector.dameng.MapBackedPartition;
import org.devlive.connector.dameng.logminer.LogMinerStreamingChangeEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The task to work for fetching data of Dameng table stream split. */
@Internal
public class DamengStreamFetchTask implements FetchTask<SourceSplitBase> {

    private final StreamSplit split;
    private volatile boolean taskRunning = false;

    public DamengStreamFetchTask(StreamSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        DamengSourceFetchTaskContext sourceFetchContext = (DamengSourceFetchTaskContext) context;
        taskRunning = true;
        RedoLogSplitReadTask redoLogSplitReadTask =
                new RedoLogSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getEventDispatcher(),
                        sourceFetchContext.getWaterMarkDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getSourceConfig().getOriginDbzConnectorConfig(),
                        sourceFetchContext.getStreamingChangeEventSourceMetrics(),
                        split);
        StoppableChangeEventSourceContext changeEventSourceContext =
                new StoppableChangeEventSourceContext();
        redoLogSplitReadTask.execute(
                changeEventSourceContext,
                sourceFetchContext.getPartition(),
                sourceFetchContext.getOffsetContext());
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public StreamSplit getSplit() {
        return split;
    }

    @Override
    public void close() {
        taskRunning = false;
    }

    /**
     * A wrapped task to read all redo log for table and also supports read bounded (from
     * lowWatermark to highWatermark) redo log.
     */
    public static class RedoLogSplitReadTask extends LogMinerStreamingChangeEventSource {

        private static final Logger LOG = LoggerFactory.getLogger(RedoLogSplitReadTask.class);
        private final StreamSplit redoLogSplit;
        EventDispatcher<MapBackedPartition, TableId> eventDispatcher;
        private final WatermarkDispatcher watermarkDispatcher;
        private final ErrorHandler errorHandler;
        private final DamengConnectorConfig connectorConfig;
        private final DamengConnection connection;

        private final DamengDatabaseSchema schema;

        private final DamengStreamingChangeEventSourceMetrics metrics;

        public RedoLogSplitReadTask(
                DamengConnectorConfig connectorConfig,
                DamengConnection connection,
                EventDispatcher<MapBackedPartition, TableId> eventDispatcher,
                WatermarkDispatcher watermarkDispatcher,
                ErrorHandler errorHandler,
                DamengDatabaseSchema schema,
                Configuration jdbcConfig,
                DamengStreamingChangeEventSourceMetrics metrics,
                StreamSplit redoLogSplit) {
            super(
                    connectorConfig,
                    connection,
                    eventDispatcher,
                    errorHandler,
                    Clock.SYSTEM,
                    schema,
                    new DamengTaskContext(connectorConfig, schema),
                    jdbcConfig,
                    metrics);
            this.redoLogSplit = redoLogSplit;
            this.eventDispatcher = eventDispatcher;
            this.watermarkDispatcher = watermarkDispatcher;
            this.errorHandler = errorHandler;
            this.connectorConfig = connectorConfig;
            this.connection = connection;
            this.metrics = metrics;
            this.schema = schema;
        }

        @Override
        public void execute(
                ChangeEventSourceContext context,
                MapBackedPartition partition,
                DamengOffsetContext offsetContext) {
            super.execute(context, partition, offsetContext);
        }
    }
}
