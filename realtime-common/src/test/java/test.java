import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class test {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkasource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("realtime_topic_db")
                .setGroupId("topic_group")
                // 为了保证消费的精准一致性

                .setStartingOffsets(OffsetsInitializer.earliest())
                // 如果使用Flink提供的SimpleStringSchema对String进行反序列化，如果消息为空，会报错
                // 自定义序列化
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if( message != null){
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        // 3.3 消费数据，封装为流

        DataStreamSource<String> kafkaStrDs =
                env.fromSource(kafkasource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        kafkaStrDs.print();

        env.execute();

    }
}
