package ElasticSearch;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client=createClient();

        KafkaConsumer<String, String> consumer=createConsumer();

        while(true) {
            ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord record: records){
                //Insert into ElasticSearch

                //2 strategies
                //kafka generic id
                //String id=record.topic()+record.partition()+record.offset();

                //twitter feed specific id
                String id=extractIdFromTweet(record.value().toString());

                JSONObject jsonRecord=new JSONObject();
                String msg=record.value().toString();
                //passing id to make consumer idempotent
                IndexRequest indexRequest=new IndexRequest("twitter", "tweets", id)
                        .source(msg, XContentType.JSON);
                IndexResponse response=client.index(indexRequest);
                String responseId= response.getId();
                System.out.println(responseId);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //client.close();
    }

    public static KafkaConsumer<String, String> createConsumer() {
        String bootstrapServers="127.0.0.1:9092";
        String groupId="kafka-elastic-search";
        String topic="twitter_tweets";

        //create consumer configs
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //Read data from the earliest
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    private static JsonParser jsonParser=new JsonParser();

    public static String extractIdFromTweet(String tweetJson) {
        //gson library
        String id=JsonParser.parseString(tweetJson).getAsJsonObject().get("id_str").getAsString();
        return id;

    }

    public static RestHighLevelClient createClient() {
        String hostname="kafka-cluster-8489686516.us-east-1.bonsaisearch.net";
        String username="kjucx3hi03";
        String password="mbmtt6r5jp";

        final CredentialsProvider credentialsProvider= new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder= RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client=new RestHighLevelClient(builder);
        return client;
    }

}
