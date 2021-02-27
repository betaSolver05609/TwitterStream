package ElasticSearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class ElasticSearchConsumer {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client=createClient();
        String jsonString="{\"foo\": \"bar\"}";
        IndexRequest indexRequest=new IndexRequest("twitter", "tweets")
                .source(jsonString, XContentType.JSON);
        IndexResponse response=client.index(indexRequest);
        String id= response.getId();
        System.out.println(id);

        client.close();
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
