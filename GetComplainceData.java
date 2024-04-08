package com.wayfair.partner.account.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.graphql.dgs.client.CustomGraphQLClient;
import com.netflix.graphql.dgs.client.GraphQLClient;
import com.netflix.graphql.dgs.client.GraphQLResponse;
import com.netflix.graphql.dgs.client.HttpResponse;
import com.opencsv.CSVWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.time.Duration;
import java.util.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;

import static reactor.core.publisher.Mono.just;

public class GetComplainceData {

    private final WebClient webClient;

    ObjectMapper objectMapper;

    private final String URL ="https://admin.wayfair.com/federation/chaos/service-to-service";

    String csvFilePath = "complaince_result_1.csv";
    private final  String query = "query ($salesAccountId: ID!) "
            + "{ legacySupplier { legacySupplierBySalesAccountId(salesAccountId: $salesAccountId) { id } } }";
    private final String QUERY = "query LegalEntityById($legalEntityId: String!) {\n" +
            "  legalEntity {\n" +
            "    legalEntityById(legalEntityId: $legalEntityId) {\n" +
            "      legalEntityId\n" +
            "      salesAccounts {\n" +
            "        status\n" +
            "        statusDetails {\n" +
            "          hasPaymentTerms\n" +
            "          hasSignedSPA\n" +
            "          isCOICompliant\n" +
            "          isKYBStatusCompliant\n" +
            "          isManualComplianceCheckCompliant\n" +
            "        }\n" +
            "        salesAccountId\n" +
            "        legacySupplier {\n" +
            "          legacySupplierId\n" +
            "        }\n" +
            "      }  \n" +
            "    }\n" +
            "  }\n" +
            "}";

    public GetComplainceData() {
        this.webClient = WebClient.builder()
                .baseUrl(URL)
                .build();
        this.objectMapper = new ObjectMapper();
    }

    public void getSupplier(){
        // Path to the CSV file
        String csvFilePath = "legalentity.csv";

        writeToCSV(new String[]{"SUID","Legal Entity","Sales Account" ,"Status","hasPaymentTerms","hasSignedSPA","isCOICompliant","isKYBStatusCompliant","isManualComplianceCheckCompliant"},false);

        try {
            // Create a reader for the CSV file
            Reader reader = new FileReader(csvFilePath);

            // Create a CSV parser
            org.apache.commons.csv.CSVParser csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);

            // Iterate through each record in the CSV file
            for (CSVRecord csvRecord : csvParser) {
                // Accessing values by column index

                String column1 =csvRecord.get(0);
                backfill(column1);
            }

            // Close the CSV parser
            csvParser.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public void backfill(String legalEntityId ){

        GraphQLResponse res= null;
        res = executeQuery(String.format(QUERY ),getHeaders(),getVaribales(legalEntityId));
        if( res.hasErrors()){
            System.out.println("Retrying the request");
            backfill(legalEntityId);
        }
        LinkedHashMap legalEntity = res.extractValue("legalEntity.legalEntityById");
        if(legalEntity ==null){
            return;
        }

        List<LinkedHashMap> salesAccountList= (List<LinkedHashMap>) legalEntity.get("salesAccounts");
            for (Map salesAccount : salesAccountList) {
                if(salesAccount == null){
                    continue;
                }
                String status = (String) salesAccount.get("status");
                LinkedHashMap statusDetails= (LinkedHashMap) salesAccount.get("statusDetails");
                LinkedHashMap legacySupplier= (LinkedHashMap) salesAccount.get("legacySupplier");
                int suID = (Integer) legacySupplier.get("legacySupplierId");
                writeToCSV(new String[]{String.valueOf(suID),legalEntityId , (String) salesAccount.get("salesAccountId"),status,
                        String.valueOf(statusDetails.get("hasPaymentTerms")),String.valueOf(statusDetails.get("hasSignedSPA")),
                                String.valueOf(statusDetails.get("isCOICompliant")),String.valueOf(statusDetails.get("isKYBStatusCompliant")),
                                        String.valueOf(statusDetails.get("isManualComplianceCheckCompliant"))},true);
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
    private static Map<String, Object> getVaribales(String legalEntityId) {
        Map<String, Object> variables = new HashMap<>();
        variables.put("legalEntityId",legalEntityId);
        return variables;
    }

    public static void main(String[] args) {
        GetComplainceData ref= new  GetComplainceData();
        ref.getSupplier();


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
}
