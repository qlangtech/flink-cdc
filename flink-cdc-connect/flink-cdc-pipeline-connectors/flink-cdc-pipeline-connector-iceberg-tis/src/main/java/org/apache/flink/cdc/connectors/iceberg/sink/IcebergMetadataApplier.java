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

package org.apache.flink.cdc.connectors.iceberg.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.cdc.connectors.iceberg.sink.utils.IcebergTypeUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/** A {@link MetadataApplier} for Apache Iceberg. */
public class IcebergMetadataApplier implements MetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataApplier.class);

    private transient Catalog catalog;

    private final Map<String, String> catalogOptions;

    // currently, we set table options for all tables using the same options.
    private final Map<String, String> tableOptions;

    private final Map<TableId, List<String>> partitionMaps;

    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

    public IcebergMetadataApplier(Map<String, String> catalogOptions) {
        this(catalogOptions, new HashMap<>(), new HashMap<>());
    }

    public IcebergMetadataApplier(
            Map<String, String> catalogOptions,
            Map<String, String> tableOptions,
            Map<TableId, List<String>> partitionMaps) {
        this.catalogOptions = catalogOptions;
        this.tableOptions = tableOptions;
        this.partitionMaps = partitionMaps;
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        if (catalog == null) {
            catalog =
                    CatalogUtil.buildIcebergCatalog(
                            this.getClass().getSimpleName(), catalogOptions, new Configuration());
        }
        SchemaChangeEventVisitor.visit(
                schemaChangeEvent,
                addColumnEvent -> {
                    applyAddColumn(addColumnEvent);
                    return null;
                },
                alterColumnTypeEvent -> {
                    applyAlterColumnType(alterColumnTypeEvent);
                    return null;
                },
                createTableEvent -> {
                    applyCreateTable(createTableEvent);
                    return null;
                },
                dropColumnEvent -> {
                    applyDropColumn(dropColumnEvent);
                    return null;
                },
                dropTableEvent -> {
                    throw new UnsupportedSchemaChangeEventException(dropTableEvent);
                },
                renameColumnEvent -> {
                    applyRenameColumn(renameColumnEvent);
                    return null;
                },
                truncateTableEvent -> {
                    throw new UnsupportedSchemaChangeEventException(truncateTableEvent);
                });
    }

    private void applyCreateTable(CreateTableEvent event) {
        try {
            TableIdentifier tableIdentifier = TableIdentifier.parse(event.tableId().identifier());

            // Step1: Build Schema.
            org.apache.flink.cdc.common.schema.Schema cdcSchema = event.getSchema();
            List<Types.NestedField> columns = new ArrayList<>();
            Set<Integer> identifierFieldIds = new HashSet<>();
            for (int index = 0; index < event.getSchema().getColumnCount(); index++) {
                columns.add(
                        IcebergTypeUtils.convertCdcColumnToIcebergField(
                                index, (PhysicalColumn) cdcSchema.getColumns().get(index)));
                if (cdcSchema.primaryKeys().contains(cdcSchema.getColumns().get(index).getName())) {
                    identifierFieldIds.add(index);
                }
            }

            // Step2: Build partition spec.
            Schema icebergSchema = new Schema(columns, identifierFieldIds);
            List<String> partitionColumns = cdcSchema.partitionKeys();
            if (partitionMaps.containsKey(event.tableId())) {
                partitionColumns = partitionMaps.get(event.tableId());
            }
            PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
            for (String partitionColumn : partitionColumns) {
                // 分区项支持 "{transform}:{column}" 形式（如 day:create_time），不带前缀则按 identity 处理。
                // TIS 侧的时间分区（year/month/day/hour）走的就是这个编码，见 TimePartition#createPartitionSpec
                applyPartition(builder, partitionColumn);
            }
            PartitionSpec partitionSpec = builder.build();
            if (!catalog.tableExists(tableIdentifier)) {
                // 建表属性 = sink 级 tableOptions 打底，再被本表 schema 携带的 options 覆盖。
                // flink-cdc 的 Schema 自带 options 字段（Paimon 连接器就是从这里取建表属性的），
                // 上游 Iceberg 连接器没有读它，导致上游下发的表属性（文件格式、压缩编码、
                // 目标文件大小等，见 TIS 的 createTabOpts）在建表时被静默丢弃
                Map<String, String> tableProps = new HashMap<>(tableOptions);
                if (cdcSchema.options() != null) {
                    tableProps.putAll(cdcSchema.options());
                }
                catalog.createTable(tableIdentifier, icebergSchema, partitionSpec, tableProps);
            }
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    /**
     * 把一个分区项应用到 {@link PartitionSpec.Builder} 上。
     * <p>
     * 支持 {@code transform:column} 形式（year / month / day / hour / identity），
     * 不写 transform 时按 identity 处理，与上游 flink-cdc 的行为保持一致。
     */
    private static void applyPartition(PartitionSpec.Builder builder, String partitionColumn) {
        int colonPos = partitionColumn.indexOf(':');
        if (colonPos < 0) {
            builder.identity(partitionColumn);
            return;
        }
        String transform = partitionColumn.substring(0, colonPos).trim().toLowerCase(Locale.ROOT);
        String columnName = partitionColumn.substring(colonPos + 1).trim();
        if ("year".equals(transform)) {
            builder.year(columnName);
        } else if ("month".equals(transform)) {
            builder.month(columnName);
        } else if ("day".equals(transform)) {
            builder.day(columnName);
        } else if ("hour".equals(transform)) {
            builder.hour(columnName);
        } else if ("identity".equals(transform)) {
            builder.identity(columnName);
        } else {
            throw new IllegalArgumentException("illegal partition transform:" + partitionColumn);
        }
    }

    private void applyAddColumn(AddColumnEvent event) {
        TableIdentifier tableIdentifier = TableIdentifier.parse(event.tableId().identifier());
        try {
            Table table = catalog.loadTable(tableIdentifier);
            applyAddColumnEventWithPosition(table, event);
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyAddColumnEventWithPosition(Table table, AddColumnEvent event)
            throws SchemaEvolveException {

        try {
            UpdateSchema updateSchema = table.updateSchema();
            for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
                Column addColumn = columnWithPosition.getAddColumn();
                String columnName = addColumn.getName();
                String columnComment = addColumn.getComment();
                Type icebergType =
                        FlinkSchemaUtil.convert(
                                DataTypeUtils.toFlinkDataType(addColumn.getType())
                                        .getLogicalType());
                switch (columnWithPosition.getPosition()) {
                    case FIRST:
                        updateSchema.addColumn(columnName, icebergType, columnComment);
                        table.updateSchema().moveFirst(columnName);
                        break;
                    case LAST:
                        updateSchema.addColumn(columnName, icebergType, columnComment);
                        break;
                    case BEFORE:
                        updateSchema.addColumn(columnName, icebergType, columnComment);
                        updateSchema.moveBefore(
                                columnName, columnWithPosition.getExistedColumnName());
                        break;
                    case AFTER:
                        checkNotNull(
                                columnWithPosition.getExistedColumnName(),
                                "Existing column name must be provided for AFTER position");
                        updateSchema.addColumn(columnName, icebergType, columnComment);
                        updateSchema.moveAfter(
                                columnName, columnWithPosition.getExistedColumnName());
                        break;
                    default:
                        throw new SchemaEvolveException(
                                event,
                                "Unknown column position: " + columnWithPosition.getPosition());
                }
            }
            updateSchema.commit();
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyDropColumn(DropColumnEvent event) {
        try {
            UpdateSchema updateSchema =
                    catalog.loadTable(TableIdentifier.parse(event.tableId().identifier()))
                            .updateSchema();
            event.getDroppedColumnNames().forEach(updateSchema::deleteColumn);
            updateSchema.commit();
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyRenameColumn(RenameColumnEvent event) {
        try {
            UpdateSchema updateSchema =
                    catalog.loadTable(TableIdentifier.parse(event.tableId().identifier()))
                            .updateSchema();
            event.getNameMapping().forEach(updateSchema::renameColumn);
            updateSchema.commit();
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyAlterColumnType(AlterColumnTypeEvent event) {
        try {
            UpdateSchema updateSchema =
                    catalog.loadTable(TableIdentifier.parse(event.tableId().identifier()))
                            .updateSchema();
            event.getTypeMapping()
                    .forEach(
                            (name, newType) -> {
                                Type.PrimitiveType type =
                                        FlinkSchemaUtil.convert(
                                                        DataTypeUtils.toFlinkDataType(newType)
                                                                .getLogicalType())
                                                .asPrimitiveType();
                                updateSchema.updateColumn(name, type);
                            });
            updateSchema.commit();
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        this.enabledSchemaEvolutionTypes = schemaEvolutionTypes;
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return enabledSchemaEvolutionTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return Sets.newHashSet(
                SchemaChangeEventType.CREATE_TABLE,
                SchemaChangeEventType.ADD_COLUMN,
                SchemaChangeEventType.DROP_COLUMN,
                SchemaChangeEventType.RENAME_COLUMN,
                SchemaChangeEventType.ALTER_COLUMN_TYPE);
    }

    @Override
    public void close() {
        catalog = null;
    }
}
