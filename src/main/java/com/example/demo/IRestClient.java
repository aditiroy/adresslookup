package com.example.demo;

import java.net.URI;
import java.net.http.HttpResponse;

public interface IRestClient {

    HttpResponse<String> get(final URI uri);

}
