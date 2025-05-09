package com.example.demo;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.*;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class ExcelBatchWriter {
    private static final int MAX_ROWS_PER_FILE = 100000;
    private static final int BATCH_SIZE = 1000;
    String baseFilePath = "src/main/resources/output/usaddresses.xlsx";

    public void appendAndSplitExcel(List<AddressDetail> addressDetailList) throws IOException {
        int fileIndex = getLastFileIndex(baseFilePath);
        String currentFilePath = getFilePath(baseFilePath, fileIndex);

        String state = addressDetailList.get(0).getState();
        String county = addressDetailList.get(0).getCounty();
        String sheetName = state+"_"+county;

        File currentFile = new File(currentFilePath);
        XSSFWorkbook workbook;
        Sheet sheet;
        int rowCounter;

        if (currentFile.exists()) {
            try (FileInputStream fis = new FileInputStream(currentFile)) {
                  workbook = new XSSFWorkbook(fis);
                  sheet = workbook.getSheet(sheetName);
                  if(sheet == null){
                      sheet = workbook.createSheet(sheetName);
                      createHeader(sheet, workbook);
                  }
                  rowCounter = sheet.getLastRowNum();
            }
        } else {
            workbook = new XSSFWorkbook();
            sheet = workbook.createSheet(sheetName);
            createHeader(sheet, workbook);
            rowCounter = 0;
        }

        for (int i = 0; i < addressDetailList.size(); i++) {
            if (rowCounter >= MAX_ROWS_PER_FILE) {
                saveWorkbook(workbook, currentFilePath);

                fileIndex++;
                currentFilePath = getFilePath(baseFilePath, fileIndex);
                workbook = new XSSFWorkbook();
                sheet = workbook.createSheet(sheetName);
                createHeader(sheet, workbook);
                rowCounter = 0;
            }

            AddressDetail ad = addressDetailList.get(i);
            Row row = sheet.createRow(++rowCounter);
            fillRow(row, ad);

            if (i % BATCH_SIZE == 0) {
                System.gc();
            }
        }

        saveWorkbook(workbook, currentFilePath);
    }

    private String getFilePath(String basePath, int index) {
        return basePath.replace(".xlsx", "_" + index + ".xlsx");
    }

    private int getLastFileIndex(String basePath) {
        File dir = new File(basePath).getParentFile();
        String baseName = new File(basePath).getName().replace(".xlsx", "");

        File[] matchingFiles = dir.listFiles((d, name) -> name.matches(baseName + "_\\d+\\.xlsx"));

        if (matchingFiles == null || matchingFiles.length == 0) return 1;

        return Arrays.stream(matchingFiles)
                .map(f -> f.getName().replace(".xlsx", ""))
                .mapToInt(name -> Integer.parseInt(name.replace(baseName + "_", "")))
                .max().orElse(1);
    }

    private void createHeader(Sheet sheet, Workbook workbook) {
        Row headerRow = sheet.createRow(0);
        String[] headers = {"REGION", "STATE", "COUNTY", "CITY", "NUMBER", "STREET", "POSTALCODE",
                "LONGITUDE", "LATITUDE", "GEOCENSUS_STATE", "GEOCENSUS_COUNTY", "GEOCENSUS_MUNICIPALITY",
                "GEOCENSUS_CITY", "GEOCENSUS_STREET", "GEOCENSUS_STARTING_HOUSE_NUMBER",
                "GEOCENSUS_ENDING_HOUSE_NUMBER", "GEOCENSUS_POSTALCODE", "GEOCENSUS_LONGITUDE",
                "GEOCENSUS_LATITUDE"};

        CellStyle headerStyle = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setBold(true);
        headerStyle.setFont(font);

        for (int i = 0; i < headers.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers[i]);
            cell.setCellStyle(headerStyle);
        }
    }

    private void fillRow(Row row, AddressDetail ad) {
        row.createCell(0).setCellValue(ad.getRegion());
        row.createCell(1).setCellValue(ad.getState());
        row.createCell(2).setCellValue(ad.getCounty());
        row.createCell(3).setCellValue(ad.getCity());
        row.createCell(4).setCellValue(ad.getNumber());
        row.createCell(5).setCellValue(ad.getStreet());
        row.createCell(6).setCellValue(ad.getPostcode());
        row.createCell(7).setCellValue(ad.getLongitude());
        row.createCell(8).setCellValue(ad.getLatitude());
        row.createCell(9).setCellValue(ad.getCensusGeoCoderState());
        row.createCell(10).setCellValue(ad.getCensusGeoCoderCounty());
        row.createCell(11).setCellValue(ad.getCensusGeoCoderMunicipality());
        row.createCell(12).setCellValue(ad.getCensusGeoCoderCity());
        row.createCell(13).setCellValue(ad.getCensusGeoCoderStreet());
        row.createCell(14).setCellValue(ad.getCensusGeoCoderFromAddressNumber());
        row.createCell(15).setCellValue(ad.getCensusGeoCoderToAddressNumber());
        row.createCell(16).setCellValue(ad.getCensusGeoCoderZip());
        row.createCell(17).setCellValue(ad.getCensusGeoCoderLongitude());
        row.createCell(18).setCellValue(ad.getCensusGeoCoderLatitude());
    }

    private void saveWorkbook(XSSFWorkbook workbook, String filePath) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            workbook.write(fos);
        }
    }
}




