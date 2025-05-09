package com.example.demo;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@Slf4j
public class RestClient implements IRestClient {

    @Override
    public HttpResponse<String> get(URI uri) {
        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .GET()
                .build();
        HttpResponse<String> response = null;
        try {
            response   = client.send(request, HttpResponse.BodyHandlers.ofString());
            log.info("Status Code: {}" ,response.statusCode());
            log.info("Response Body:{} ", response.body());
        } catch (IOException | InterruptedException e) {
           log.error("Error occurred: {}",e.getMessage());
        }

    return response;
    }


}
