package org.apache.flink.connector.clickhouse;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.connector.clickhouse.util.CommonConstant;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.IDENTIFIER;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.*;
import static org.apache.flink.connector.clickhouse.util.ClickHouseUtil.getClickHouseProperties;

/** A {@link DynamicTableSinkFactory} for discovering {@link ClickHouseDynamicTableSink}. */
public class ClickHouseDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    public ClickHouseDynamicTableFactory() {}

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);

        CommonConstant.appendOpKind = config.get(APPEND_OP);
        CommonConstant.cdasFlag = config.get(CDAS_FLAG);

        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        String[] primaryKeys =
                catalogTable
                        .getResolvedSchema()
                        .getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .map(keys -> keys.toArray(new String[0]))
                        .orElse(new String[0]);
        return new ClickHouseDynamicTableSink(
                getDmlOptions(config),
                primaryKeys,
                catalogTable.getPartitionKeys().toArray(new String[0]),
                context.getPhysicalRowDataType());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);

        Properties clickHouseProperties =
                getClickHouseProperties(context.getCatalogTable().getOptions());
        return new ClickHouseDynamicTableSource(
                getReadOptions(config), clickHouseProperties, context.getPhysicalRowDataType());
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(DATABASE_NAME);
        optionalOptions.add(USE_LOCAL);
        optionalOptions.add(SINK_BATCH_SIZE);
        optionalOptions.add(SINK_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_UPDATE_STRATEGY);
        optionalOptions.add(SINK_PARTITION_STRATEGY);
        optionalOptions.add(SINK_PARTITION_KEY);
        optionalOptions.add(SINK_IGNORE_DELETE);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(CATALOG_IGNORE_PRIMARY_KEY);
        optionalOptions.add(SCAN_PARTITION_COLUMN);
        optionalOptions.add(SCAN_PARTITION_NUM);
        optionalOptions.add(SCAN_PARTITION_LOWER_BOUND);
        optionalOptions.add(SCAN_PARTITION_UPPER_BOUND);
        optionalOptions.add(APPEND_OP);
        optionalOptions.add(CDAS_FLAG);
        return optionalOptions;
    }

    private void validateConfigOptions(ReadableConfig config) {
        if (config.get(SINK_PARTITION_STRATEGY).partitionKeyNeeded
                && !config.getOptional(SINK_PARTITION_KEY).isPresent()) {
            throw new IllegalArgumentException(
                    "A partition key must be provided for hash partition strategy");
        } else if (config.getOptional(USERNAME).isPresent()
                ^ config.getOptional(PASSWORD).isPresent()) {
            throw new IllegalArgumentException(
                    "Either all or none of username and password should be provided");
        } else if (config.getOptional(SCAN_PARTITION_COLUMN).isPresent()
                ^ config.getOptional(SCAN_PARTITION_NUM).isPresent()
                ^ config.getOptional(SCAN_PARTITION_LOWER_BOUND).isPresent()
                ^ config.getOptional(SCAN_PARTITION_UPPER_BOUND).isPresent()) {
            throw new IllegalArgumentException(
                    "Either all or none of partition configs should be provided");
        }
    }

    private ClickHouseDmlOptions getDmlOptions(ReadableConfig config) {
        return new ClickHouseDmlOptions.Builder()
                .withUrl(config.get(URL))
                .withUsername(config.get(USERNAME))
                .withPassword(config.get(PASSWORD))
                .withDatabaseName(config.get(DATABASE_NAME))
                .withTableName(config.get(TABLE_NAME))
                .withBatchSize(config.get(SINK_BATCH_SIZE))
                .withFlushInterval(config.get(SINK_FLUSH_INTERVAL))
                .withMaxRetries(config.get(SINK_MAX_RETRIES))
                .withUseLocal(config.get(USE_LOCAL))
                .withUpdateStrategy(config.get(SINK_UPDATE_STRATEGY))
                .withPartitionStrategy(config.get(SINK_PARTITION_STRATEGY))
                .withPartitionKey(config.get(SINK_PARTITION_KEY))
                .withIgnoreDelete(config.get(SINK_IGNORE_DELETE))
                .withParallelism(config.get(SINK_PARALLELISM))
                .build();
    }

    private ClickHouseReadOptions getReadOptions(ReadableConfig config) {
        return new ClickHouseReadOptions.Builder()
                .withUrl(config.get(URL))
                .withUsername(config.get(USERNAME))
                .withPassword(config.get(PASSWORD))
                .withDatabaseName(config.get(DATABASE_NAME))
                .withTableName(config.get(TABLE_NAME))
                .withUseLocal(config.get(USE_LOCAL))
                .withPartitionColumn(config.get(SCAN_PARTITION_COLUMN))
                .withPartitionNum(config.get(SCAN_PARTITION_NUM))
                .withPartitionLowerBound(config.get(SCAN_PARTITION_LOWER_BOUND))
                .withPartitionUpperBound(config.get(SCAN_PARTITION_UPPER_BOUND))
                .build();
    }
}
