package TwitterStream;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    String consumerKey = "FjAcckNdR4NhwKATJ0qD1no2P";
    String consumerSecret= "855H8d15A6Kg1KcQy2ZGLZc7x4c8YL9SuaKt0tUGfYwFMdKQl9";
    String token="313379436-yRnMYeH3DrnvJxrYEh6oFP70JWxH4FnNdkjVIiEd";
    String secret="gIrRMqFfr0MvXRRC4qVc0LDHcaHyiZ5bxi9ONSBmeuXTK";

    public TwitterProducer() {

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        //Create a twitter client
        Client client=createTwitterClient(msgQueue);
        client.connect();

        //Create a Kafka producer
        KafkaProducer<String, String> producer=createKafkaProducer();

        //Loop to send tweets oto kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null)
                producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e!=null)
                        {
                            System.out.println("Something bad happened");
                            e.printStackTrace();
                        }
                    }
                });
        }

        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
         client.stop();
         producer.close();
         System.out.println("System Shut Down");
        }));

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("india");
        hosebirdEndpoint.trackTerms(terms);
        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
        
    }

    public KafkaProducer<String, String> createKafkaProducer(){
        String bootstrapServers="127.0.0.1:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);
        return producer;
    }
}
