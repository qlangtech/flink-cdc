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

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * TIS 定制部分的回归测试。
 *
 * <p>这三处行为与上游 flink-cdc Iceberg 连接器不同，且都被 TIS 的增量链路依赖，改动时必须能被这里拦下：
 *
 * <ol>
 *   <li>分区项支持 {@code {transform}:{column}} 编码（TIS 的时间分区 year/month/day/hour）
 *   <li>建表属性读取 Schema 自带的 options（TIS 的 {@code createTabOpts()} 经 SchemaEmitterFunction 注入）
 *   <li>目标文件大小等写参数由建表属性决定，而不是连接器内置常量
 * </ol>
 */
public class IcebergMetadataApplierTISTest {

    @TempDir public static java.nio.file.Path temporaryFolder;

    private Map<String, String> catalogOptions() {
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put(
                CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
        catalogOptions.put(
                CatalogProperties.WAREHOUSE_LOCATION,
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString());
        catalogOptions.put(CatalogProperties.CACHE_ENABLED, "false");
        return catalogOptions;
    }

    private static Schema schema() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.BIGINT().notNull())
                .physicalColumn("create_time", DataTypes.TIMESTAMP())
                .primaryKey("id")
                .build();
    }

    /** TIS 的时间分区编码必须被翻译成 Iceberg 的 transform，而不是当作 identity 列名 */
    @Test
    public void testCreateTableWithTransformPartition() {
        Map<String, String> catalogOptions = catalogOptions();
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());

        TableId tableId = TableId.parse("test.partitioned_table");
        Map<TableId, java.util.List<String>> partitionMaps = new HashMap<>();
        partitionMaps.put(tableId, Collections.singletonList("day:create_time"));

        IcebergMetadataApplier applier =
                new IcebergMetadataApplier(catalogOptions, new HashMap<>(), partitionMaps);
        applier.applySchemaChange(new CreateTableEvent(tableId, schema()));

        Table table = catalog.loadTable(TableIdentifier.parse(tableId.identifier()));
        PartitionSpec spec = table.spec();
        assertThat(spec.isPartitioned()).isTrue();
        assertThat(spec.fields()).hasSize(1);
        assertThat(spec.fields().get(0).transform().toString()).isEqualTo("day");
        assertThat(spec.schema().findField(spec.fields().get(0).sourceId()).name())
                .isEqualTo("create_time");
    }

    /** 不带冒号的旧形式仍按 identity 处理，避免破坏上游行为 */
    @Test
    public void testCreateTableWithIdentityPartition() {
        Map<String, String> catalogOptions = catalogOptions();
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());

        TableId tableId = TableId.parse("test.identity_table");
        Map<TableId, java.util.List<String>> partitionMaps = new HashMap<>();
        partitionMaps.put(tableId, Collections.singletonList("id"));

        IcebergMetadataApplier applier =
                new IcebergMetadataApplier(catalogOptions, new HashMap<>(), partitionMaps);
        applier.applySchemaChange(new CreateTableEvent(tableId, schema()));

        PartitionSpec spec =
                catalog.loadTable(TableIdentifier.parse(tableId.identifier())).spec();
        assertThat(spec.fields()).hasSize(1);
        assertThat(spec.fields().get(0).transform().toString()).isEqualTo("identity");
    }

    /** 非法 transform 必须尽早失败，而不是静默退化成 identity */
    @Test
    public void testIllegalTransformPartition() {
        Map<String, String> catalogOptions = catalogOptions();
        TableId tableId = TableId.parse("test.illegal_table");
        Map<TableId, java.util.List<String>> partitionMaps = new HashMap<>();
        partitionMaps.put(tableId, Collections.singletonList("week:create_time"));

        IcebergMetadataApplier applier =
                new IcebergMetadataApplier(catalogOptions, new HashMap<>(), partitionMaps);
        assertThatThrownBy(() -> applier.applySchemaChange(new CreateTableEvent(tableId, schema())))
                .hasMessageContaining("illegal partition transform");
    }

    /**
     * Schema 携带的 options 必须落到建表属性上。
     *
     * <p>TIS 侧的 writer 把文件格式、压缩编码与目标文件大小都放在 {@code createTabOpts()} 里，
     * 由 SchemaEmitterFunction 注入 Schema.options；上游连接器不读它，这些配置就会静默失效，
     * 表退化成连接器内置的 parquet 默认值。
     */
    @Test
    public void testCreateTableAppliesSchemaOptions() {
        Map<String, String> catalogOptions = catalogOptions();
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());

        TableId tableId = TableId.parse("test.orc_table");
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("create_time", DataTypes.TIMESTAMP())
                        .primaryKey("id")
                        .option(TableProperties.DEFAULT_FILE_FORMAT, "orc")
                        .option(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "536870912")
                        .build();

        IcebergMetadataApplier applier = new IcebergMetadataApplier(catalogOptions);
        applier.applySchemaChange(new CreateTableEvent(tableId, cdcSchema));

        Table table = catalog.loadTable(TableIdentifier.parse(tableId.identifier()));
        assertThat(table.properties())
                .containsEntry(TableProperties.DEFAULT_FILE_FORMAT, "orc")
                .containsEntry(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "536870912");
    }

    /** sink 级 tableOptions 打底、Schema.options 覆盖：同名键以 Schema 为准 */
    @Test
    public void testSchemaOptionsOverrideSinkTableOptions() {
        Map<String, String> catalogOptions = catalogOptions();
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());

        TableId tableId = TableId.parse("test.override_table");
        Map<String, String> sinkTableOptions = new HashMap<>();
        sinkTableOptions.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
        // 只在 sink 级出现、Schema 里没有的键必须保留下来。
        // 注意不能用 format-version：那是表元数据字段，不会出现在 table.properties() 里
        sinkTableOptions.put(TableProperties.PARQUET_COMPRESSION, "gzip");

        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .primaryKey("id")
                        .option(TableProperties.DEFAULT_FILE_FORMAT, "avro")
                        .build();

        IcebergMetadataApplier applier =
                new IcebergMetadataApplier(catalogOptions, sinkTableOptions, new HashMap<>());
        applier.applySchemaChange(new CreateTableEvent(tableId, cdcSchema));

        assertThat(catalog.loadTable(TableIdentifier.parse(tableId.identifier())).properties())
                .containsEntry(TableProperties.DEFAULT_FILE_FORMAT, "avro")
                .containsEntry(TableProperties.PARQUET_COMPRESSION, "gzip");
    }

    /** 仅当表不存在时建表，重复的 CreateTableEvent 不能覆盖已有表结构 */
    @Test
    public void testCreateTableIsIdempotent() {
        Map<String, String> catalogOptions = catalogOptions();
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());

        TableId tableId = TableId.parse("test.existed_table");
        // 预置一张表，列结构与事件不同
        catalog.createTable(
                TableIdentifier.parse(tableId.identifier()),
                new org.apache.iceberg.Schema(
                        org.apache.iceberg.types.Types.NestedField.required(
                                1, "only_col", org.apache.iceberg.types.Types.StringType.get())),
                PartitionSpec.unpartitioned());

        IcebergMetadataApplier applier = new IcebergMetadataApplier(catalogOptions);
        applier.applySchemaChange(new CreateTableEvent(tableId, schema()));

        Table table = catalog.loadTable(TableIdentifier.parse(tableId.identifier()));
        assertThat(table.schema().columns()).hasSize(1);
        assertThat(table.schema().findField("only_col")).isNotNull();
    }
}
