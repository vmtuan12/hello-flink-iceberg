package org.tuan;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.flink.maintenance.api.JdbcLockFactory;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshots;

import java.time.Duration;
import java.util.*;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("databus-kafka-bootstrap.kafka.svc:9092")
                .setGroupId("tuan-test-3")
                .setTopics("flink_topic_test_2")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> dataStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.noWatermarks(),
            "kafka-source"
        );

        String s3Endpoint = "http://minio.minio.svc:9000";
        String s3AccessKey = "";
        String s3SecretKey = "";

        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("type", "iceberg");
        catalogOptions.put("catalog-type", "hive");
        catalogOptions.put("uri", "thrift://-datawarehouse-metastore.hive.svc:9083");
        catalogOptions.put("warehouse", "s3a://-datawarehouse/iceberg/.warehouse/");
        catalogOptions.put("s3.endpoint", s3Endpoint);
        catalogOptions.put("s3.access-key-id", s3AccessKey);
        catalogOptions.put("s3.secret-access-key", s3SecretKey);
        catalogOptions.put("s3.region", "aws-global");
        catalogOptions.put("s3.path-style-access", "true");
        catalogOptions.put("fs.native-s3.enabled", "true");
        catalogOptions.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalogOptions.put("catalog-impl", "org.apache.iceberg.hive.HiveCatalog");

        Configuration hadopConf = new Configuration();
        hadopConf.set("fs.s3a.access.key", s3AccessKey);
        hadopConf.set("fs.s3a.secret.key", s3SecretKey);
        hadopConf.set("fs.s3a.endpoint", s3Endpoint);
        hadopConf.set("fs.s3a.path.style.access", "true");
        hadopConf.set("fs.native-s3.enabled", "true");
//        hadopConf.set("fs.s3a.aws.credentials.provider", "org.apache.flink.fs.s3.common.token.DynamicTemporaryAWSCredentialsProvider");

        dataStream.print();
        CatalogLoader catalogLoader = CatalogLoader.hive("_datawarehouse", hadopConf, catalogOptions);

        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier tableName = TableIdentifier.of("test_geo", "flink_test_upsert");
        if (!catalog.tableExists(tableName)) {
            List<Types.NestedField> columns = List.of(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())
            );
            Schema schema = new Schema(columns, Set.<Integer>of(1));

            catalog.createTable(tableName, schema);
        }


        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableName);
        DataStream<RowData> rowDataDataStream = dataStream.map(new RawStringToRowDataMapper());

        Map<String, String> jdbcProps = new HashMap<>();
        jdbcProps.put("jdbc.user", "");
        jdbcProps.put("jdbc.password", "");

        TriggerLockFactory lockFactory = new JdbcLockFactory(
                "jdbc:postgresql://:6432/iceberg",
                String.format("%s.%s.%s", catalog.name(), tableName.namespace().toString(), tableName.name()),
                jdbcProps
        );
        TableMaintenance.forTable(env, tableLoader, lockFactory)
                .uidSuffix("production-maintenance")
                .rateLimit(Duration.ofMinutes(1))
                .lockCheckDelay(Duration.ofSeconds(30))
                .parallelism(1)
                // Daily snapshot cleanup
                .add(ExpireSnapshots.builder()
                        .cleanExpiredMetadata(true)
                        .maxSnapshotAge(Duration.ofMinutes(1))
                        .retainLast(1))
                .append();

        FlinkSink.forRowData(rowDataDataStream)
                .tableLoader(tableLoader)
                .upsert(true)
                .append();

        env.execute("tuan-kafka-app");
    }
}