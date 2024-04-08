package com.wayfair.partner.account.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.graphql.dgs.client.CustomGraphQLClient;
import com.netflix.graphql.dgs.client.GraphQLClient;
import com.netflix.graphql.dgs.client.GraphQLResponse;
import com.netflix.graphql.dgs.client.HttpResponse;
import com.opencsv.CSVParser;
import com.opencsv.CSVWriter;
import graphql.ExecutionInput;
import io.swagger.models.auth.In;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.*;
import lombok.val;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;

import static reactor.core.publisher.Mono.just;

public class SPABackFill {


    private final WebClient webClient;

    ObjectMapper objectMapper;

    private final String URL ="https://admin.wayfair.com/federation/chaos/service-to-service";

    String csvFilePath = "result_spa_2022.csv";
    private final  String query = "query ($salesAccountId: ID!) "
            + "{ legacySupplier { legacySupplierBySalesAccountId(salesAccountId: $salesAccountId) { id } } }";
    private final String QUERY = "query ($supplierFilterInput: SupplierFilterInput!) {\n" +
            "  legacySuppliersByFilters(supplierFilterInput: $supplierFilterInput) {\n" +
            "  legacySupplierId, \n"+
            "    salesAccount {\n" +
            "      salesAccountId\n" +
            "      legalEntity {\n" +
            "        legalEntityId\n" +
            "      }     \n" +
            "      statusDetails {\n" +
            "        hasSignedSPA\n" +
            "      }\n" +
            "    }\n" +
            "    \n" +
            "  }\n" +
            "}";

    public SPABackFill() {
        this.webClient = WebClient.builder()
                .baseUrl(URL)
                .build();
        this.objectMapper = new ObjectMapper();
    }

    public void getSupplier(){
        // Path to the CSV file
        String csvFilePath = "spa_2022.csv";
        int count=5;

        try {
            // Create a reader for the CSV file
            Reader reader = new FileReader(csvFilePath);

            // Create a CSV parser
            org.apache.commons.csv.CSVParser csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);

            List<Integer> suIds = new ArrayList<>();
            // Iterate through each record in the CSV file
            for (CSVRecord csvRecord : csvParser) {
                // Accessing values by column index

                int column1 = Integer.parseInt(csvRecord.get(0));
                suIds.add(column1);
                count --;
                if(count == 0){
                    System.out.println("Calling the back fill");
                    backfill(suIds);
                    count=5;
                    suIds = new ArrayList<>();
                }
            }

            // Close the CSV parser
            csvParser.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void backfill(List<Integer> suid ){

        GraphQLResponse res= null;
            res = executeQuery(String.format(QUERY ),getHeaders(),getVaribales(suid));
        if( res.hasErrors()){
            System.out.println("Retrying the request");
            backfill(suid);
        }
        List<LinkedHashMap> salesAccountList = res.extractValue("legacySuppliersByFilters");

        for (Map account : salesAccountList) {

            LinkedHashMap salesAccount= (LinkedHashMap) account.get("salesAccount");
            if(salesAccount == null){
                continue;
            }
            LinkedHashMap legalEntity= (LinkedHashMap) salesAccount.get("legalEntity");
            LinkedHashMap statusDetails= (LinkedHashMap) salesAccount.get("statusDetails");

            int suId = (Integer) account.get("legacySupplierId");


            writeToCSV(new String[]{String.valueOf(suId),"2(COMPLETE)","SPA", (String) salesAccount.get("salesAccountId"),
                    (String) legalEntity.get("legalEntityId"),
                    String.valueOf(statusDetails.get("hasSignedSPA")), ""},true);

        }


    }

    private void writeToCSV(String[] data, boolean append) {
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFilePath, append))) {
            writer.writeNext(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @NotNull
    private static MultiValueMap<String, String> getHeaders() {
        MultiValueMap<String, String> requestHeaders= new LinkedMultiValueMap<>();
        requestHeaders.put("Accept", Collections.singletonList("application/json"));
        requestHeaders.put("Accept-Encoding", Collections.singletonList("gzip"));
        requestHeaders.put("Content-Type", Collections.singletonList("application/json"));
        requestHeaders.put("apollographql-client-name", Collections.singletonList("apollo-sudio"));
        requestHeaders.put("apollographql-client-version", Collections.singletonList("prod-service-to-service"));
        requestHeaders.put("Authorization", Collections.singletonList("Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6InBlRG5BMWVVVGRVQU00YVdjU3FnZm40ZEJhbFZCYnJ4R2ZEU0ZQYXVQbG8iLCJ0eXAiOiJKV1QiLCJ4NXQiOiJrY2ExZDVxbmg4OVQ2ZzlNQzlaOHAzTkFBRFEiLCJqa3UiOiJodHRwczovL3Nzby5hdXRoLndheWZhaXIuY29tLy53ZWxsLWtub3duL29wZW5pZC1jb25maWd1cmF0aW9uL2p3a3MifQ.eyJuYmYiOjE3MTE0MjE5OTUsImV4cCI6MTcxMTUwODM5NiwiaWF0IjoxNzExNDIxOTk2LCJpc3MiOiJodHRwczovL3Nzby5hdXRoLndheWZhaXIuY29tLyIsImF1ZCI6WyJhdXRoX2lkZW50aXR5IiwiY29tcGxpYW5jZS1iYXJyaWVycy1tYW5hZ2VyIiwiY29tcGxpYW5jZS1yZXN0cmljdGlvbnMiLCJjb21wbGlhbmNlLXJ1bGUtbWFuYWdlciIsImNvbXBsaWFuY2Utc2Nhbi1vcmNoZXN0cmF0b3IiLCJsZWdhY3ktc3VwcGxpZXItZW50aXRpZXMtYWRhcHRlciIsInBhcnRuZXItYWNjb3VudC1zZXJ2aWNlIiwicGgtcGF5bWVudHMtYW5kLXRlcm1zLWZzbSIsInNhZS1jb21wbGlhbmNlLWt5Yi1saXZlLXN1cHBsaWVyLWV4cCIsInN1cHBsaWVyLWNyZWF0aW9uLW9yY2hlc3RyYXRvciIsInN1cHBsaWVyLXN5bmNocm9uaXNlci1zZXJ2aWNlIl0sImNsaWVudF9pZCI6IjVPdW05YnJDSDl3V0N6VmMteTJQWGciLCJjbGllbnRfbHJwcyI6IjEwMDAiLCJjbGllbnRfbmFtZSI6IktZQiBtaWxlc3RvbmVfNCIsInN1YiI6IjVPdW05YnJDSDl3V0N6VmMteTJQWGdAY2xpZW50cyIsImp0aSI6IkM3MTYyREJBM0ZDQzZENDg1MkYxNjVGNkMxRTk3MkMzIiwic2NvcGUiOiJhdXRoLndheWZhaXIuY29tOmNsaWVudF9yZWFkIGNvbXBsaWFuY2UtYmFycmllcnMtbWFuYWdlcjpkZWZhdWx0X3Njb3BlIGNvbXBsaWFuY2UtcmVzdHJpY3Rpb25zOmRlZmF1bHRfc2NvcGUgY29tcGxpYW5jZS1ydWxlLW1hbmFnZXI6ZGVmYXVsdF9zY29wZSBjb21wbGlhbmNlLXNjYW4tb3JjaGVzdHJhdG9yOmRlZmF1bHRfc2NvcGUgbGVnYWN5LXN1cHBsaWVyLWVudGl0aWVzLWFkYXB0ZXI6cmVhZCBsZWdhY3ktc3VwcGxpZXItZW50aXRpZXMtYWRhcHRlcjp3cml0ZSBwYXJ0bmVyLWFjY291bnQtc2VydmljZTphZG1pbiBwYXJ0bmVyLWFjY291bnQtc2VydmljZTphdXRoel9zY29wZSBwYXJ0bmVyLWFjY291bnQtc2VydmljZTpkZWZhdWx0X3Njb3BlIHBoLXBheW1lbnRzLWFuZC10ZXJtcy1mc206ZGVmYXVsdF9zY29wZSBzYWUtY29tcGxpYW5jZS1reWItbGl2ZS1zdXBwbGllci1leHA6ZGVmYXVsdF9zY29wZSBzdXBwbGllci1jcmVhdGlvbi1vcmNoZXN0cmF0b3I6ZGVmYXVsdF9zY29wZSBzdXBwbGllci1zeW5jaHJvbmlzZXItc2VydmljZTpkZWZhdWx0X3Njb3BlIn0.D87TOtl93gmme0HJQ01WgoLRHRHvLwJRjYPKhBD_agf2tjuIgiEIv0p38mv0z5-4PiwWb4Dpr2JKb9K01LG9vDrsgWc38eAlna1VOA-dTFcsUcEmRcdPfPUBCfupoc5tf0D7tjZIIhxdcWTnBaDx2IKfIzsJXvVb1FnylgdvDjGwKAAUyMDDQsn8ZrRZ3gUiFiVRRVfQ_N-K5UcKlbgcETVuPSYAyi0XGZEMNlcfg2EbJn3XoqlaOuE7EhMsXFgA-JvvmFRcnW0rLKZKgHJqvlsxYepUnRb-s14MrNpCTNccujfOXAGrzpPprRz5jQGzlHk0G8T52G0q-_osPlP_Z80KdvT3ysUwtK1k9UgDkNiXSV2JNMd3zNHewgvveVzG9lKYgGWpLdVsA-JnCx2arDsAah4uqwjOtXWygFEn3ft7SZTsIumL9wjfs0sQBP7TF5IoXJk1TspqG_lGkD2v3eNcAa1qPgLBp_MMLuhxPFi0a5aExAp8T7yk1V8TGKeqC3JgpVtdlLj5RtmEw21v42eKpKzYzzBj22_91qFgEBnPCqEBC_uWtS2r1VEbw_rSjo7gmqBgWdcajEAP2LuB_uaVofWUJzeOVIQLgmlgey-kH9SWABCQyiOZnAS7oO6snDlbOElzVSomeWzIZk4dXRrTazdZf-P5_gshEq8q0_U"));
        return requestHeaders;
    }

    @NotNull
    private static Map<String, Object> getVaribales(List<Integer> suid) {
        Map<String, Object> variables = new HashMap<>();
        Map<String, List<Integer>> variableInput = new HashMap<>();
        variableInput.put("supplierIds",suid);
        variables.put("supplierFilterInput",variableInput);
        return variables;
    }

    public GraphQLResponse executeQuery(String query,MultiValueMap<String, String> requestHeaders, Map<String, Object> variables){

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

    public static void main(String[] args) {
        SPABackFill ref= new  SPABackFill();
        ref.writeToCSV(new String[]{"SUID", "Current Status in Legacy DB" , "Complaince", "Sales Account ","Legal Entity" ,"Previous PAS Status" , "Current PAS Status"},false);
        ref.getSupplier();


    }

}
