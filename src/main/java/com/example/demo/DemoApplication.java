package com.example.demo;

import java.net.URISyntaxException;

import lombok.extern.slf4j.Slf4j;

@Slf4j


public class DemoApplication {

	public static void main(String[] args) {
		OpenAddressGeoJsonProcessor openAddressGeoJsonProcessor = new OpenAddressGeoJsonProcessor();

        openAddressGeoJsonProcessor.processTheAddressFile();
	}

}
