package com.example.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Slf4j
public class AddressDetail {
    String city;
    String state;
    String street;
    String number;
    String district;
    String region;
    String postcode;
    String county;
    double longitude;
    double latitude;
    String censusGeoCoderCity;
    String censusGeoCoderState;
    String censusGeoCoderZip;
    String censusGeoCoderStreet;
    double censusGeoCoderLongitude;
    double censusGeoCoderLatitude;
    String censusGeoCoderCounty;
    String censusGeoCoderMunicipality;
    String censusGeoCoderFromAddressNumber;
    String censusGeoCoderToAddressNumber;
}
