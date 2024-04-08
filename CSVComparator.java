package com.wayfair.partner.account.service;

import com.opencsv.CSVWriter;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.FileReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

public class CSVComparator {

    static String  csvFilePath = "common_payment_term_allowance_2023_to_now.csv";

    public static void main(String[] args) {
        // Paths to the CSV files
        String csvFilePath1 = "result_allowance_2023_to_now.csv";
        String csvFilePath2 = "result_payment_terms_2023_to_now.csv";
        writeToCSV(new String[]{"SUID"},false);

        try {
            // Read CSV file 1
            Set<String> columnValues1 = readColumnValues(csvFilePath1, "SUID");

            // Read CSV file 2
            Set<String> columnValues2 = readColumnValues(csvFilePath2, "SUID");

            // Find common values
            Set<String> commonValues = new HashSet<>(columnValues1);
            commonValues.retainAll(columnValues2);

           for (String val :commonValues){
               writeToCSV(new String[]{String.valueOf(val)},true);
           }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeToCSV(String[] data, boolean append) {
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFilePath, append))) {
            writer.writeNext(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Helper method to read column values from CSV file
    private static Set<String>
    readColumnValues(String filePath, String columnName) throws Exception {
        Set<String> columnValues = new HashSet<>();
        Reader reader = new FileReader(filePath);
        CSVParser csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
        for (CSVRecord csvRecord : csvParser) {
            String columnValue = csvRecord.get(columnName);
            if (columnValue != null && !columnValue.isEmpty()) {
                columnValues.add(columnValue);
            }
        }
        csvParser.close();
        return columnValues;
    }
}