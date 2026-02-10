package org.tuan;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("databus-kafka-bootstrap.kafka.svc:9092")
                .setGroupId("tuan-test-2")
                .setTopics("tuan_test_topic_string")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> dataStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.noWatermarks(),
            "kafka-source"
        );

        String s3Endpoint = "";
        String s3AccessKey = "";
        String s3SecretKey = "";

        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("type", "iceberg");
        catalogOptions.put("catalog-type", "hive");
        catalogOptions.put("uri", "thrift://datawarehouse-metastore.hive.svc:9083");
        catalogOptions.put("warehouse", "s3a://datawarehouse/iceberg/.warehouse/");
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
        CatalogLoader catalogLoader = CatalogLoader.hive("datawarehouse", hadopConf, catalogOptions);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of("test_geo", "tuan_dz_str"));
        DataStream<RowData> rowDataDataStream = dataStream.map(new RawStringToRowDataMapper());

        FlinkSink.forRowData(rowDataDataStream)
                .tableLoader(tableLoader)
                .append();

        env.execute("tuan-kafka-app");
    }
}