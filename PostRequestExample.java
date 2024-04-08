package com.wayfair.partner.account.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.graphql.dgs.client.CustomGraphQLClient;
import com.netflix.graphql.dgs.client.GraphQLClient;
import com.netflix.graphql.dgs.client.GraphQLResponse;
import com.netflix.graphql.dgs.client.HttpResponse;
import com.opencsv.CSVWriter;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;

import static reactor.core.publisher.Mono.just;

public class PostRequestExample {

    private static final String URL ="https://admin.wayfair.com/federation/chaos/service-to-service";

    private static final String QUERY = "query LegalEntity($legalEntityId: String!) {\n" +
            "  legalEntity {\n" +
            "    legalEntityById(legalEntityId: $legalEntityId) {\n" +
            "      salesAccounts {\n" +
            "        statusDetails {\n" +
            "          isKYBStatusCompliant\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}\n";

    public PostRequestExample() {
        this.webClient = WebClient.builder()
                .baseUrl(URL)
                .build();
    }
    private final WebClient webClient;
    static  String csvFilePath = "backfill_result_kyb_march_24_to_now.csv";

    public static void main(String[] args) {
        // Path to the CSV file
        String csvFilePath = "legalentity.csv";

        writeToCSV(new String[]{"Legal entity ", "Previous PAS  KYB Status" ,"Current PAS  KYB Status", "Backfill call"},false);


        try {
            // Create a reader for the CSV file
            Reader reader = new FileReader(csvFilePath);
            // Create a CSV parser
            org.apache.commons.csv.CSVParser csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
            // Iterate th   rough each record in the CSV file
            for (CSVRecord csvRecord : csvParser) {
                // Accessing values by column index
                String column1 = csvRecord.get(0);
                PostRequestExample r = new PostRequestExample();
                if(!r.getCurrentStatus(column1,r)){
                    post(column1);
                }
            }

            // Close the CSV parser
            csvParser.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private  boolean getCurrentStatus(String column1,PostRequestExample r) {

        GraphQLResponse res = r.executeQuery(String.format(QUERY ),getHeaders(),getVaribales(column1));
        Boolean hasSignedSPA= true;
        if( res.hasErrors()){
            System.out.println("Retrying the request");
            getCurrentStatus(column1, r);
        }

        if(res.extractValue("legalEntity") == null){
            return hasSignedSPA;
        }

        if(res.extractValue("legalEntity.legalEntityById") == null){
            return hasSignedSPA;
        }

        List<LinkedHashMap> salesAccountList = res.extractValue("legalEntity.legalEntityById.salesAccounts");

        for (Map account : salesAccountList) {

            LinkedHashMap statusDetails= (LinkedHashMap) account.get("statusDetails");
            hasSignedSPA= (Boolean) statusDetails.get("isKYBStatusCompliant");
        }
        return hasSignedSPA;
    }

    @NotNull
    private static MultiValueMap<String, String> getHeaders() {
        MultiValueMap<String, String> requestHeaders= new LinkedMultiValueMap<>();
        requestHeaders.put("Accept", Collections.singletonList("application/json"));
        requestHeaders.put("Accept-Encoding", Collections.singletonList("gzip"));
        requestHeaders.put("Content-Type", Collections.singletonList("application/json"));
        requestHeaders.put("apollographql-client-name", Collections.singletonList("apollo-sudio"));
        requestHeaders.put("apollographql-client-version", Collections.singletonList("prod-service-to-service"));
        requestHeaders.put("Authorization", Collections.singletonList("Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6InBlRG5BMWVVVGRVQU00YVdjU3FnZm40ZEJhbFZCYnJ4R2ZEU0ZQYXVQbG8iLCJ0eXAiOiJKV1QiLCJ4NXQiOiJ0LWNGRFk1bzZSU1JFRWdjM0lmWWVFX2lDNzQiLCJqa3UiOiJodHRwczovL3Nzby5hdXRoLndheWZhaXIuY29tLy53ZWxsLWtub3duL29wZW5pZC1jb25maWd1cmF0aW9uL2p3a3MifQ.eyJuYmYiOjE3MTIzMDA4ODcsImV4cCI6MTcxMjM4NzI4OCwiaWF0IjoxNzEyMzAwODg4LCJpc3MiOiJodHRwczovL3Nzby5hdXRoLndheWZhaXIuY29tLyIsImF1ZCI6WyJhdXRoX2lkZW50aXR5IiwiY29tcGxpYW5jZS1iYXJyaWVycy1tYW5hZ2VyIiwiY29tcGxpYW5jZS1yZXN0cmljdGlvbnMiLCJjb21wbGlhbmNlLXJ1bGUtbWFuYWdlciIsImNvbXBsaWFuY2Utc2Nhbi1vcmNoZXN0cmF0b3IiLCJsZWdhY3ktc3VwcGxpZXItZW50aXRpZXMtYWRhcHRlciIsInBhcnRuZXItYWNjb3VudC1zZXJ2aWNlIiwicGgtcGF5bWVudHMtYW5kLXRlcm1zLWZzbSIsInNhZS1jb21wbGlhbmNlLWt5Yi1saXZlLXN1cHBsaWVyLWV4cCIsInN1cHBsaWVyLWNyZWF0aW9uLW9yY2hlc3RyYXRvciIsInN1cHBsaWVyLXN5bmNocm9uaXNlci1zZXJ2aWNlIl0sImNsaWVudF9pZCI6IjVPdW05YnJDSDl3V0N6VmMteTJQWGciLCJjbGllbnRfbHJwcyI6IjEwMDAiLCJjbGllbnRfbmFtZSI6IktZQiBtaWxlc3RvbmVfNCIsInN1YiI6IjVPdW05YnJDSDl3V0N6VmMteTJQWGdAY2xpZW50cyIsImp0aSI6IjU0RUVERTQzNTNCN0ZDMUEzNDFBREQ4NTMzMTg2RDZGIiwic2NvcGUiOiJhdXRoLndheWZhaXIuY29tOmNsaWVudF9yZWFkIGNvbXBsaWFuY2UtYmFycmllcnMtbWFuYWdlcjpkZWZhdWx0X3Njb3BlIGNvbXBsaWFuY2UtcmVzdHJpY3Rpb25zOmRlZmF1bHRfc2NvcGUgY29tcGxpYW5jZS1ydWxlLW1hbmFnZXI6ZGVmYXVsdF9zY29wZSBjb21wbGlhbmNlLXNjYW4tb3JjaGVzdHJhdG9yOmRlZmF1bHRfc2NvcGUgbGVnYWN5LXN1cHBsaWVyLWVudGl0aWVzLWFkYXB0ZXI6cmVhZCBsZWdhY3ktc3VwcGxpZXItZW50aXRpZXMtYWRhcHRlcjp3cml0ZSBwYXJ0bmVyLWFjY291bnQtc2VydmljZTphZG1pbiBwYXJ0bmVyLWFjY291bnQtc2VydmljZTphdXRoel9zY29wZSBwYXJ0bmVyLWFjY291bnQtc2VydmljZTpkZWZhdWx0X3Njb3BlIHBoLXBheW1lbnRzLWFuZC10ZXJtcy1mc206ZGVmYXVsdF9zY29wZSBzYWUtY29tcGxpYW5jZS1reWItbGl2ZS1zdXBwbGllci1leHA6ZGVmYXVsdF9zY29wZSBzdXBwbGllci1jcmVhdGlvbi1vcmNoZXN0cmF0b3I6ZGVmYXVsdF9zY29wZSBzdXBwbGllci1zeW5jaHJvbmlzZXItc2VydmljZTpkZWZhdWx0X3Njb3BlIn0.msetWAr_RsjDG2llVJTnxtEd87hAn5ghosbE0DCn4Wji0nFFK7NxoXV-rReG0LYxQ9-eYGZ2ji4dOA0q0DRk35r6FVfloYll77F3PWWu2r4_0FVQWxWtih65kdm8uZXPOsqULITIqDHuHljJxfGk7LOT4LH2dSEWy7LhuAYBwCST1XEgx3X9f77KT7zY0-ZdQvR0AaEVI5_7Z3yZ150H0JTZvkHOwwOR9LyJ7B9h9h8QyirC6VpS_K8v06yrBvE19sDrfdnmBm6p74q5115hdICaAi-8zDL9seFCkfacLEwr84XBFvK8svkm0ioFpGdLd1PeXFqTwtwBtYejjzLQmRMOoyJCqgYeUNs00V07fPugYYWU9Kplp8HoVnUUe_Ds7x6kTgJn3sDjftj_DuBLgsj33jKkRE3welCGmmXvLZPLfGAf_3A-GVUGsoJFaBDCxfWOqHQsKZw7UEbBZox4i492qND5APwHAUy9_SJHR3AgVV9ESCUq7u46NXu7LGYf_30Xl5Uhcgtlfx0UTmu2Q2mUPTPwTyw2PIXvusyXa6rut8_P6-H4ww4vU9GjOJA7Rhoe9dlZwuuRwBSrLjQ4B3cYu3hSEfIZ7gpg4A0fSn5IJB6TpE5vxG7sAY9SChE_CIzMTLcInTMY0Wz7lMQBKMNryrDaHhY7EgrwT3H6qDE"));
        return requestHeaders;
    }

    @NotNull
    private static Map<String, Object> getVaribales(String suid) {
        Map<String, Object> variables = new HashMap<>();
        variables.put("legalEntityId",suid);
        return variables;
    }

    public GraphQLResponse executeQuery(String query, MultiValueMap<String, String> requestHeaders, Map<String, Object> variables){

        GraphQLClient graphQLClient = new CustomGraphQLClient("/graphql", (url, headers, body) -> {
            try {
                String responseBody = webClient.post()
                        .uri(url)
                        .headers(h -> h.addAll(requestHeaders))
                        .body(just(body), String.class)
                        .retrieve()
                        .bodyToMono(String.class)
                        .timeout(Duration.ofMillis(5000000))
                        .block();
                return new HttpResponse(HttpStatus.OK.value(), responseBody);
            } catch (Exception exception) {
                throw exception;
            }
        });
        return graphQLClient.executeQuery(query, variables);
    }


    private static void writeToCSV(String[] data, boolean append) {
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFilePath, append))) {
            writer.writeNext(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void post(String legalEntityId){
        try {
            // URL endpoint for the POST request
            String url = "http://kube-partner-account-service.service.intraiad1.consul.csnzoo.com/legal-entity/status";

            // Create an HTTP client
            CloseableHttpClient httpClient = HttpClients.createDefault();

            // Create a POST request
            HttpPost httpPost = new HttpPost(url);

            // Set request headers
            httpPost.addHeader("Content-Type", "application/json");
            httpPost.addHeader("Authorization", "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6InBlRG5BMWVVVGRVQU00YVdjU3FnZm40ZEJhbFZCYnJ4R2ZEU0ZQYXVQbG8iLCJ0eXAiOiJKV1QiLCJ4NXQiOiJ0LWNGRFk1bzZSU1JFRWdjM0lmWWVFX2lDNzQiLCJqa3UiOiJodHRwczovL3Nzby5hdXRoLndheWZhaXIuY29tLy53ZWxsLWtub3duL29wZW5pZC1jb25maWd1cmF0aW9uL2p3a3MifQ.eyJuYmYiOjE3MTIzMDA4ODcsImV4cCI6MTcxMjM4NzI4OCwiaWF0IjoxNzEyMzAwODg4LCJpc3MiOiJodHRwczovL3Nzby5hdXRoLndheWZhaXIuY29tLyIsImF1ZCI6WyJhdXRoX2lkZW50aXR5IiwiY29tcGxpYW5jZS1iYXJyaWVycy1tYW5hZ2VyIiwiY29tcGxpYW5jZS1yZXN0cmljdGlvbnMiLCJjb21wbGlhbmNlLXJ1bGUtbWFuYWdlciIsImNvbXBsaWFuY2Utc2Nhbi1vcmNoZXN0cmF0b3IiLCJsZWdhY3ktc3VwcGxpZXItZW50aXRpZXMtYWRhcHRlciIsInBhcnRuZXItYWNjb3VudC1zZXJ2aWNlIiwicGgtcGF5bWVudHMtYW5kLXRlcm1zLWZzbSIsInNhZS1jb21wbGlhbmNlLWt5Yi1saXZlLXN1cHBsaWVyLWV4cCIsInN1cHBsaWVyLWNyZWF0aW9uLW9yY2hlc3RyYXRvciIsInN1cHBsaWVyLXN5bmNocm9uaXNlci1zZXJ2aWNlIl0sImNsaWVudF9pZCI6IjVPdW05YnJDSDl3V0N6VmMteTJQWGciLCJjbGllbnRfbHJwcyI6IjEwMDAiLCJjbGllbnRfbmFtZSI6IktZQiBtaWxlc3RvbmVfNCIsInN1YiI6IjVPdW05YnJDSDl3V0N6VmMteTJQWGdAY2xpZW50cyIsImp0aSI6IjU0RUVERTQzNTNCN0ZDMUEzNDFBREQ4NTMzMTg2RDZGIiwic2NvcGUiOiJhdXRoLndheWZhaXIuY29tOmNsaWVudF9yZWFkIGNvbXBsaWFuY2UtYmFycmllcnMtbWFuYWdlcjpkZWZhdWx0X3Njb3BlIGNvbXBsaWFuY2UtcmVzdHJpY3Rpb25zOmRlZmF1bHRfc2NvcGUgY29tcGxpYW5jZS1ydWxlLW1hbmFnZXI6ZGVmYXVsdF9zY29wZSBjb21wbGlhbmNlLXNjYW4tb3JjaGVzdHJhdG9yOmRlZmF1bHRfc2NvcGUgbGVnYWN5LXN1cHBsaWVyLWVudGl0aWVzLWFkYXB0ZXI6cmVhZCBsZWdhY3ktc3VwcGxpZXItZW50aXRpZXMtYWRhcHRlcjp3cml0ZSBwYXJ0bmVyLWFjY291bnQtc2VydmljZTphZG1pbiBwYXJ0bmVyLWFjY291bnQtc2VydmljZTphdXRoel9zY29wZSBwYXJ0bmVyLWFjY291bnQtc2VydmljZTpkZWZhdWx0X3Njb3BlIHBoLXBheW1lbnRzLWFuZC10ZXJtcy1mc206ZGVmYXVsdF9zY29wZSBzYWUtY29tcGxpYW5jZS1reWItbGl2ZS1zdXBwbGllci1leHA6ZGVmYXVsdF9zY29wZSBzdXBwbGllci1jcmVhdGlvbi1vcmNoZXN0cmF0b3I6ZGVmYXVsdF9zY29wZSBzdXBwbGllci1zeW5jaHJvbmlzZXItc2VydmljZTpkZWZhdWx0X3Njb3BlIn0.msetWAr_RsjDG2llVJTnxtEd87hAn5ghosbE0DCn4Wji0nFFK7NxoXV-rReG0LYxQ9-eYGZ2ji4dOA0q0DRk35r6FVfloYll77F3PWWu2r4_0FVQWxWtih65kdm8uZXPOsqULITIqDHuHljJxfGk7LOT4LH2dSEWy7LhuAYBwCST1XEgx3X9f77KT7zY0-ZdQvR0AaEVI5_7Z3yZ150H0JTZvkHOwwOR9LyJ7B9h9h8QyirC6VpS_K8v06yrBvE19sDrfdnmBm6p74q5115hdICaAi-8zDL9seFCkfacLEwr84XBFvK8svkm0ioFpGdLd1PeXFqTwtwBtYejjzLQmRMOoyJCqgYeUNs00V07fPugYYWU9Kplp8HoVnUUe_Ds7x6kTgJn3sDjftj_DuBLgsj33jKkRE3welCGmmXvLZPLfGAf_3A-GVUGsoJFaBDCxfWOqHQsKZw7UEbBZox4i492qND5APwHAUy9_SJHR3AgVV9ESCUq7u46NXu7LGYf_30Xl5Uhcgtlfx0UTmu2Q2mUPTPwTyw2PIXvusyXa6rut8_P6-H4ww4vU9GjOJA7Rhoe9dlZwuuRwBSrLjQ4B3cYu3hSEfIZ7gpg4A0fSn5IJB6TpE5vxG7sAY9SChE_CIzMTLcInTMY0Wz7lMQBKMNryrDaHhY7EgrwT3H6qDE");


            // Create JSON payload for the POST request
            // Create JSON payload for the POST request
            String requestBody = "{ \"legalEntityId\" : \""+ legalEntityId +"\", \"status\" : \"PASSED\", \"type\": \"KYB\" }";

            // Set request body
            StringEntity requestEntity = new StringEntity(requestBody, ContentType.APPLICATION_JSON);
            httpPost.setEntity(requestEntity);

            // Execute the request
            CloseableHttpResponse response = httpClient.execute(httpPost);

            // Get response code
            int statusCode = response.getCode();
            System.out.println("Response Code: " + statusCode);

            // Read response body
            HttpEntity responseEntity = response.getEntity();
            BufferedReader reader = new BufferedReader(new InputStreamReader(responseEntity.getContent()));
            StringBuilder responseBody = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                responseBody.append(line);
            }
            reader.close();

            writeToCSV(new String[]{legalEntityId , "FALSE","TRUE",String.valueOf(statusCode)},true);
            // Print response body
            System.out.println("Response Body:  " + legalEntityId+ responseBody);

            // Close the response and HTTP client
            response.close();
            httpClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
