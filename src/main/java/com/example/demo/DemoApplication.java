package com.example.demo;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@SpringBootApplication
public class DemoApplication {
public static void main(String[] args) throws IOException {
// Create the Elasticsearch client

    org.elasticsearch.client.RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200, "http"))
            .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                    .disableAuthCaching()
                    .setDefaultHeaders(List.of(new BasicHeader("X-Elastic-Product", "Elasticsearch"),
                            new BasicHeader("Content-Type", "application/json"))));

			RestClientTransport transport = new RestClientTransport(restClientBuilder.build(), new JacksonJsonpMapper() {
			});
			ElasticsearchClient client = new ElasticsearchClient(transport);


        Map<String, Object> data = new HashMap<>();
        data.put("name", "John Doe");
        data.put("email", "johndoe@example.com");
        data.put("address", "123 Main St");
        data.put("phoneNumber", "555-555-5555");

        // Convert Map to JSON
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonData = objectMapper.writeValueAsString(data);

        // List to hold bulk operations
        List<BulkOperation> bulkOperations = new ArrayList<>();

        // Generate fake data and add to bulk request
        for (int i = 1; i <= 10; i++) { // Change this number to add more documents
            // Create a map or JSON for the document

            // Add to bulk operation
            bulkOperations.add(new BulkOperation.Builder()
                    .index(new IndexOperation.Builder<>()
                            .index("my-index") // replace with your index name
                            .id(String.valueOf(i))
                            .document(jsonData)

                            .build())
                    .build()
            );
        }

        // Create bulk request and execute
        BulkRequest bulkRequest = new BulkRequest.Builder().operations(bulkOperations).build();
        BulkResponse bulkResponse = client.bulk(bulkRequest);

        // Check for errors
        if (bulkResponse.errors()) {
            System.out.println("Bulk request had errors");
            bulkResponse.items().forEach(item -> {
                if (item.error() != null) {
                    System.out.println("Error indexing document ID: " + item.id());
                    System.out.println(item.error().reason());
                }
            });
        } else {
            System.out.println("Bulk request successful!");
        }
}

    }