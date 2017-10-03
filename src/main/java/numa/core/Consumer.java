package numa.core;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by alonso on 2/10/17.
 */
public class Consumer {
    private KafkaConsumer<Long, String> consumer = null;
    private Properties props = new Properties();

    public Consumer(String bootstrap) {
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", "consumer-tutorial");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
    }

    public void consume(String elasticHost, Integer elasticPort, String... topics) {
        consumer.subscribe(Arrays.asList(topics));
        final int giveUp = 100;   int noRecordsCount = 0;
        RestClient restClient = RestClient.builder(
                new HttpHost(elasticHost, elasticPort, "http")).build();
        RestHighLevelClient client = new RestHighLevelClient(restClient);

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach((ConsumerRecord<Long, String> record) -> {
                IndexRequest request = new IndexRequest(
                        "twitter",
                        record.topic());
                request.source(record.value(), XContentType.JSON);
                try {
                    IndexResponse indexResponse = client.index(request);
                    System.out.println("Indexed on " + record.topic());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }
}
