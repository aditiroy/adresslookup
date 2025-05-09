package com.example.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class OpenAddressGeoJsonProcessor {
    //Optimised code flow
    public static final String rootDirectoryPath = "src/main/resources/us";
    public static final int BATCH_SIZE = 1000;
    private static final ExecutorService executor = Executors.newFixedThreadPool(30);
    private static final ObjectMapper mapper = new ObjectMapper();

    IRestClient restClient;
    ExcelBatchWriter frw;

    public OpenAddressGeoJsonProcessor() {
        restClient = new RestClient();
        frw = new ExcelBatchWriter();
    }

    public void processTheAddressFile() {

        List<Future<AddressDetail>> futures = new ArrayList<>();
        List<AddressDetail> buffer = new ArrayList<>();

        try (Stream<Path> pathStream = Files.walk(Paths.get(rootDirectoryPath))) {
            pathStream.filter(Files::isRegularFile) // Filter out directories
                    .forEach(path -> {
                        String filePath = path.toString();
                        String fileName = path.getFileName().toString();
                        if (fileName.endsWith(".geojson") && fileName.contains("addresses") && fileName.contains("county")) {
                            log.info("Reading file: {}", fileName);
                            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                                String line;

                                while ((line = reader.readLine()) != null) {
                                    JsonNode feature = mapper.readTree(line);
                                    futures.add(executor.submit(() -> callCensusApi(feature, path)));

                                    if (futures.size() >= BATCH_SIZE) {
                                        for (Future<AddressDetail> f : futures) {
                                            buffer.add(f.get());
                                        }
                                        frw.appendAndSplitExcel(buffer);
                                        futures.clear();
                                        buffer.clear();
                                    }
                                }

                                //Final batch
                                if (!futures.isEmpty()) {
                                    for (Future<AddressDetail> f : futures) {
                                        buffer.add(f.get());
                                    }
                                    frw.appendAndSplitExcel(buffer);
                                    futures.clear();
                                    buffer.clear();
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            log.info("Not a valid file: {}", fileName);
                        }
                    });
        } catch (IOException e) {
            log.error("Error traversing the directory: {} ", rootDirectoryPath);
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    private AddressDetail callCensusApi(JsonNode root, Path filepath) {
        String region = filepath.getParent().getParent().getFileName().toString();
        String state = filepath.getParent().getFileName().toString();
        String fileName = filepath.getFileName().toString();
        String[] filenamePart = fileName.split("-");
        String county = filenamePart[0];

        AddressDetail address = new AddressDetail();
        address.setCounty(county);
        address.setState(state);
        address.setRegion(region);
        setAddressDetails(root, address);

        if (!StringUtils.isBlank(address.getStreet()) && ((!StringUtils.isBlank(address.getCity()) && !StringUtils.isBlank(address.getState())) || (!StringUtils.isBlank(address.getPostcode())))) {
            String baseUrl = "https://geocoding.geo.census.gov/geocoder/geographies/address";
            String StreetDetails = address.getNumber() + " " + address.getStreet();
            // Encode parameters to handle spaces and special characters
            String query = getRequestUri(StreetDetails, address);
            URI getRequest = null;
            try {
                getRequest = new URI(baseUrl + "?" + query);

                HttpResponse<String> response = restClient.get(getRequest);
                if (response.statusCode() == 200) {

                    String responseBody = response.body();

                    // Check if addressMatches exists and has at least one item
                    Object addressMatches = JsonPath.read(responseBody, "$.result.addressMatches");
                    if (addressMatches instanceof List<?> matches && !matches.isEmpty()) {

                        String municipality = JsonPath.read(responseBody, "$.result.addressMatches[0].geographies[\"County Subdivisions\"][0].NAME");
                        String geocoderCounty = JsonPath.read(responseBody, "$.result.addressMatches[0].geographies[\"Counties\"][0].NAME");
                        String geocoderResponseZipcode = JsonPath.read(responseBody, "$.result.addressMatches[0].addressComponents.zip");
                        String geocoderResponseCity = JsonPath.read(responseBody, "$.result.addressMatches[0].addressComponents.city");
                        String geocoderResponseState = JsonPath.read(responseBody, "$.result.addressMatches[0].addressComponents.state");
                        String geocoderstreetName = JsonPath.read(responseBody, "$.result.addressMatches[0].addressComponents.streetName");
                        double geoCoderlongitude = JsonPath.read(responseBody, "$.result.addressMatches[0].coordinates.x");
                        double geocoderlatitude = JsonPath.read(responseBody, "$.result.addressMatches[0].coordinates.y");
                        String geoCoderFromAddressNumber = JsonPath.read(responseBody, "$.result.addressMatches[0].addressComponents.fromAddress");
                        String geoCoderToAddressNumber = JsonPath.read(responseBody, "$.result.addressMatches[0].addressComponents.toAddress");
                        String preType = JsonPath.read(responseBody, "$.result.addressMatches[0].addressComponents.preType");
                        String preDirection = JsonPath.read(responseBody, "$.result.addressMatches[0].addressComponents.preDirection");
                        String suffixDirection = JsonPath.read(responseBody, "$.result.addressMatches[0].addressComponents.suffixDirection");
                        String suffixType = JsonPath.read(responseBody, "$.result.addressMatches[0].addressComponents.suffixType");
                        String suffixQualifier = JsonPath.read(responseBody, "$.result.addressMatches[0].addressComponents.suffixQualifier");
                        String preQualifier = JsonPath.read(responseBody, "$.result.addressMatches[0].addressComponents.preQualifier");

                        Map<String, String> components = new HashMap<>();
                        components.put("preDirection", preDirection);
                        components.put("preQualifier", preQualifier);
                        components.put("preType", preType);
                        components.put("streetName", geocoderstreetName);
                        components.put("suffixType", String.valueOf(suffixType));
                        components.put("suffixDirection", String.valueOf(suffixDirection));
                        components.put("suffixQualifier", String.valueOf(suffixQualifier));

                        String streetName = buildstreetName(components);

                        address.setCensusGeoCoderState(geocoderResponseState);
                        address.setCensusGeoCoderCity(geocoderResponseCity);
                        address.setCensusGeoCoderZip(geocoderResponseZipcode);
                        address.setCensusGeoCoderCounty(geocoderCounty);
                        address.setCensusGeoCoderMunicipality(municipality);
                        address.setCensusGeoCoderLatitude(geocoderlatitude);
                        address.setCensusGeoCoderLongitude(geoCoderlongitude);
                        address.setCensusGeoCoderStreet(streetName);
                        address.setCensusGeoCoderFromAddressNumber(geoCoderFromAddressNumber);
                        address.setCensusGeoCoderToAddressNumber(geoCoderToAddressNumber);
                    } else {
                        String noMatch = "NoMatch Found";
                        address.setCensusGeoCoderCity(noMatch);
                        address.setCensusGeoCoderState(noMatch);
                        address.setCensusGeoCoderZip(noMatch);
                        address.setCensusGeoCoderCounty(noMatch);
                        address.setCensusGeoCoderMunicipality(noMatch);
                        address.setCensusGeoCoderStreet(noMatch);
                        address.setCensusGeoCoderFromAddressNumber(noMatch);
                        address.setCensusGeoCoderToAddressNumber(noMatch);
                    }
                }
            } catch (Exception e) {
                log.error("Exception while calling the geocoding api", e.getMessage());
                address.setCensusGeoCoderCity(e.getMessage());
                address.setCensusGeoCoderState(e.getMessage());
                address.setCensusGeoCoderZip(e.getMessage());
                address.setCensusGeoCoderCounty(e.getMessage());
                address.setCensusGeoCoderMunicipality(e.getMessage());
                address.setCensusGeoCoderStreet(e.getMessage());
                address.setCensusGeoCoderFromAddressNumber(e.getMessage());
                address.setCensusGeoCoderToAddressNumber(e.getMessage());

            }
        } else {
            String callSkip = "Geo Census API call skipped";
            address.setCensusGeoCoderCity(callSkip);
            address.setCensusGeoCoderState(callSkip);
            address.setCensusGeoCoderZip(callSkip);
            address.setCensusGeoCoderCounty(callSkip);
            address.setCensusGeoCoderMunicipality(callSkip);
            address.setCensusGeoCoderStreet(callSkip);
            address.setCensusGeoCoderFromAddressNumber(callSkip);
            address.setCensusGeoCoderToAddressNumber(callSkip);
        }
        return address;
    }


    private String getRequestUri(String StreetDetails, AddressDetail address) {

        Map<String, String> params = new LinkedHashMap<>();

        if (StreetDetails != null && !StreetDetails.isEmpty()) {
            params.put("street", StreetDetails);
        }
        if (address.getCity() != null && !address.getCity().isEmpty()) {
            params.put("city", address.getCity());
        }
        if (address.getState() != null && !address.getState().isEmpty()) {
            params.put("state", address.getState());
        }
        if (address.getPostcode() != null && !address.getPostcode().isEmpty()) {
            params.put("zip", address.getPostcode());
        }

// Always include fixed parameters
        params.put("benchmark", "Public_AR_Current");
        params.put("vintage", "Current_Current");
        params.put("format", "json");
        params.put("layers", "82,22");

        return params.entrySet()
                .stream()
                .map(entry -> entry.getKey() + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));

    }

    private void setAddressDetails(JsonNode root, AddressDetail addressDetail) {
        JsonNode properties = root.get("properties");
        if (properties != null) {
            // Access individual values within 'properties'
            // String idValue = properties.has("id") ? properties.get("id").asText() : null;
            // String unitValue = properties.has("unit") ? properties.get("unit").asText() : null;
            String numberValue = properties.has("number") ? properties.get("number").asText() : null;
            String streetValue = properties.has("street") ? properties.get("street").asText() : null;
            String cityValue = properties.has("city") ? properties.get("city").asText() : null;
            String districtValue = properties.has("district") ? properties.get("district").asText() : null;
            // String regionValue = properties.has("region") ? properties.get("region").asText() : null;
            String postcodeValue = properties.has("postcode") ? properties.get("postcode").asText() : null;
            String hashValue = properties.has("hash") ? properties.get("hash").asText() : null;
            addressDetail.setCity(cityValue);
            addressDetail.setDistrict(districtValue);
            addressDetail.setStreet(streetValue);
            addressDetail.setPostcode(postcodeValue);
            addressDetail.setNumber(numberValue);

        } else {
            log.error("Error: 'properties' node not found.");
        }

        JsonNode geometry = root.get("geometry");

        if (geometry.has("type") && geometry.get("type").asText().equals("Point") && geometry.has("coordinates") && geometry.get("coordinates").isArray()) {
            JsonNode coordinatesArray = geometry.get("coordinates");
            if (coordinatesArray.size() == 2) {
                double longitude = coordinatesArray.get(0).asDouble();
                double latitude = coordinatesArray.get(1).asDouble();

                addressDetail.setLatitude(latitude);
                addressDetail.setLongitude(longitude);

            } else {
                log.error("Error: 'coordinates' array should have exactly two elements (longitude, latitude).");
            }
        } else {
            log.error("Error: Invalid JSON structure for a Point geometry with coordinates.");
        }
    }

    private String buildstreetName(Map<String, String> components) {
        //[fromAddress] [preDirection] [preQualifier] [preType] [streetName] [suffixType] [suffixDirection] [suffixQualifier], [city], [state] [zip]
        return Stream.of(
                        components.getOrDefault("preDirection", ""),
                        components.getOrDefault("preQualifier", ""),
                        components.getOrDefault("preType", ""),
                        components.getOrDefault("streetName", ""),
                        components.getOrDefault("suffixType", ""),
                        components.getOrDefault("suffixDirection", ""),
                        components.getOrDefault("suffixQualifier", "")
                )
                .filter(s -> s != null && !s.trim().isEmpty())
                .reduce((s1, s2) -> s1 + " " + s2)
                .orElse("");
    }


}






