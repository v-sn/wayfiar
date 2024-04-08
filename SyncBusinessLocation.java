package com.wayfair.partner.account.service;

import com.netflix.graphql.dgs.client.CustomGraphQLClient;
import com.netflix.graphql.dgs.client.GraphQLClient;
import com.netflix.graphql.dgs.client.GraphQLResponse;
import com.netflix.graphql.dgs.client.HttpResponse;
import com.opencsv.CSVWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;

import static reactor.core.publisher.Mono.just;


public class SyncBusinessLocation {

    private final WebClient webClient;

    private final String URL ="https://admin.wayfair.com/federation/chaos/service-to-service";

    String csvFilePath = "result.csv";
    private final  String query = "query ($salesAccountId: ID!) "
            + "{ legacySupplier { legacySupplierBySalesAccountId(salesAccountId: $salesAccountId) { id } } }";
    private final String QUERY = "query {\n" +
            "        partnerAccount {\n" +
            "          partnerAccountsAll(pageSize: %d, page: %d) {\n" +
            "             pageInfo {\n" +
            "              currentPage\n" +
            "              hasNextPage\n" +
            "              hasPreviousPage\n" +
            "            }\n" +
            "            nodes {\n" +
            "                headquarterAddress {\n" +
            "                      line1,         "+
            "                      line1         "+
            "                        }\n" +
            "                salesAccounts {\n" +
            "                  salesAccountId\n" +
            "                      legalEntity{\n" +
            "                          businessLocation {\n" +
            "                              line1\n" +
            "                              line2\n" +
            "                          }\n" +
            "                        }\n" +
            "                  legacySupplier {\n" +
            "                    legacySupplierId\n" +
            "                    origin\n" +
            "                    businessAddress1\n" +
            "                    businessAddress2\n" +
            "                  }\n" +
            "                \n" +
            "                }\n" +
            "              }\n" +
            "          }\n" +
            "        }\n" +
            "      }";

    public SyncBusinessLocation() {
        this.webClient = WebClient.builder()
                .baseUrl(URL)
                .build();
    }


    public void getNonSyncSupplierAddress(int page , int pagesize){

        var res=  executeQuery(String.format(QUERY ,pagesize ,page  ),getHeaders(),getVaribales(page,pagesize));
        compareAddress(res);
        boolean hasNextPage =
                res.extractValue("partnerAccount.partnerAccountsAll.pageInfo.hasNextPage");

        if( hasNextPage ){
            getNonSyncSupplierAddress(++page,pagesize);
        }
    }

    private void compareAddress(GraphQLResponse res) {

        List<LinkedHashMap> salesAccountList = res.extractValue("partnerAccount.partnerAccountsAll.nodes");
        for (Map account : salesAccountList) {
            List<LinkedHashMap> sales= (List<LinkedHashMap>) account.get("salesAccounts");
            LinkedHashMap hq= (LinkedHashMap) account.get("headquarterAddress");
            for (Map sale : sales) {
                String blAddress = "", suplierAddress = "";
                LinkedHashMap legacy = (LinkedHashMap) sale.get("legacySupplier");
                if (null == legacy ) {
                    continue;
                }

                suplierAddress = (String) legacy.get("businessAddress1");

                LinkedHashMap legalEntity = (LinkedHashMap) sale.get("legalEntity");
                LinkedHashMap businessLocation = (LinkedHashMap) legalEntity.get("businessLocation");

                if (null != businessLocation) {
                    blAddress = (String) businessLocation.get("line1");
                }

                if (null== blAddress || !blAddress.equals(suplierAddress)) {
                    writeToCSV(new String[]{(String) sale.get("salesAccountId"), String.valueOf(legacy.get("legacySupplierId")),
                            blAddress+ " "+ businessLocation.get("line2") == null ? "": (String) businessLocation.get("line2"),
                            (hq.get("line1") ==null ? "":  hq.get("line1") )+ " "+ (hq.get("line2") == null ? "":  (String) hq.get("line2")),
                            suplierAddress + " "+legacy.get("businessAddress2") == null ?"" :(String) legacy.get("businessAddress2") , (String) legacy.get("origin")}, true);
                }
            }
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
        requestHeaders.put("Authorization", Collections.singletonList("Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6InBlRG5BMWVVVGRVQU00YVdjU3FnZm40ZEJhbFZCYnJ4R2ZEU0ZQYXVQbG8iLCJ0eXAiOiJKV1QiLCJ4NXQiOiJrY2ExZDVxbmg4OVQ2ZzlNQzlaOHAzTkFBRFEiLCJqa3UiOiJodHRwczovL3Nzby5hdXRoLndheWZhaXIuY29tLy53ZWxsLWtub3duL29wZW5pZC1jb25maWd1cmF0aW9uL2p3a3MifQ.eyJuYmYiOjE3MTEzNjU3MjksImV4cCI6MTcxMTQ1MjEzMCwiaWF0IjoxNzExMzY1NzMwLCJpc3MiOiJodHRwczovL3Nzby5hdXRoLndheWZhaXIuY29tLyIsImF1ZCI6WyJhdXRoX2lkZW50aXR5IiwiY29tcGxpYW5jZS1iYXJyaWVycy1tYW5hZ2VyIiwiY29tcGxpYW5jZS1yZXN0cmljdGlvbnMiLCJjb21wbGlhbmNlLXJ1bGUtbWFuYWdlciIsImNvbXBsaWFuY2Utc2Nhbi1vcmNoZXN0cmF0b3IiLCJsZWdhY3ktc3VwcGxpZXItZW50aXRpZXMtYWRhcHRlciIsInBhcnRuZXItYWNjb3VudC1zZXJ2aWNlIiwicGgtcGF5bWVudHMtYW5kLXRlcm1zLWZzbSIsInNhZS1jb21wbGlhbmNlLWt5Yi1saXZlLXN1cHBsaWVyLWV4cCIsInN1cHBsaWVyLWNyZWF0aW9uLW9yY2hlc3RyYXRvciIsInN1cHBsaWVyLXN5bmNocm9uaXNlci1zZXJ2aWNlIl0sImNsaWVudF9pZCI6IjVPdW05YnJDSDl3V0N6VmMteTJQWGciLCJjbGllbnRfbHJwcyI6IjEwMDAiLCJjbGllbnRfbmFtZSI6IktZQiBtaWxlc3RvbmVfNCIsInN1YiI6IjVPdW05YnJDSDl3V0N6VmMteTJQWGdAY2xpZW50cyIsImp0aSI6Ijk2RDJERTAxOTM1QzUzNjcwNEM0RUU1MTQ0QTc0RUEyIiwic2NvcGUiOiJhdXRoLndheWZhaXIuY29tOmNsaWVudF9yZWFkIGNvbXBsaWFuY2UtYmFycmllcnMtbWFuYWdlcjpkZWZhdWx0X3Njb3BlIGNvbXBsaWFuY2UtcmVzdHJpY3Rpb25zOmRlZmF1bHRfc2NvcGUgY29tcGxpYW5jZS1ydWxlLW1hbmFnZXI6ZGVmYXVsdF9zY29wZSBjb21wbGlhbmNlLXNjYW4tb3JjaGVzdHJhdG9yOmRlZmF1bHRfc2NvcGUgbGVnYWN5LXN1cHBsaWVyLWVudGl0aWVzLWFkYXB0ZXI6cmVhZCBsZWdhY3ktc3VwcGxpZXItZW50aXRpZXMtYWRhcHRlcjp3cml0ZSBwYXJ0bmVyLWFjY291bnQtc2VydmljZTphZG1pbiBwYXJ0bmVyLWFjY291bnQtc2VydmljZTphdXRoel9zY29wZSBwYXJ0bmVyLWFjY291bnQtc2VydmljZTpkZWZhdWx0X3Njb3BlIHBoLXBheW1lbnRzLWFuZC10ZXJtcy1mc206ZGVmYXVsdF9zY29wZSBzYWUtY29tcGxpYW5jZS1reWItbGl2ZS1zdXBwbGllci1leHA6ZGVmYXVsdF9zY29wZSBzdXBwbGllci1jcmVhdGlvbi1vcmNoZXN0cmF0b3I6ZGVmYXVsdF9zY29wZSBzdXBwbGllci1zeW5jaHJvbmlzZXItc2VydmljZTpkZWZhdWx0X3Njb3BlIn0.Sw5Yk0kwShvbzkbghaQGaz_GzCXxQxec34pAJs1nMXEdggKn4A07ANG3Ge53rjq8592SWgoTWsBw8i4pBr4caPje400VQQfGNdJ95HFjmXXq6cV1Z3NT4N4nlkaLKN_vxjx_t5zq7gIDi_XWNNya1dzBgxARaSSzP0qIpd5ffZ8WbOJvwWga7hxzlyPFd1rSgn2ZhFSdadGq5dooFMGTc6AE92l2jSPLOpoDeNTAn4X0XlFNKgc60AuoE4oRt4s17ksGYqPC1pYFH3w2FsXPwQCZHU-3FjVJqpVkr5GimCYNrci8wxbGYTzd-H06FiB7mtep5VL9adWxuvMQqTRUT_CS3GUG8fxWPJJyHiNAGPuo6yFI6NSusUkbDEp27Wy7bJRbC_sizeE564Zz17pFNNxXMT0ZONFCLL7zR_sjbH5lCxtnCQrrYZDhh0ancseW_1FRKE23lhTDOxeep9adSuExJh5GidiYkWRh3RnzizExYUD0nMwiCP2EnPSbPQO1Lgl3uDTfWSSKP4zQpwPlIGBr7m3ShjVPx7hfs_ok9ZQqKco6Dzw4iM70TkTBdebKoBS9ZiatntvSAm9O3RnKPYYF28Sz7AsFgftqqYq9LiB8s9f9cnJ2LhB_Y1x19QRKPbMo4e1WkBuUJjLkv6_GZBzpS1GOKUCg_x4os5XItfg"));
        return requestHeaders;
    }

    @NotNull
    private static Map<String, String> getVaribales(int page , int pagesize) {
        Map<String, String> variables = new HashMap<>();
        variables.put("page",String.valueOf(page));
        variables.put("pageSize",String.valueOf(pagesize));
        return variables;
    }

    public GraphQLResponse executeQuery(String query,MultiValueMap<String, String> requestHeaders, Map<String, String> variables) {

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
        SyncBusinessLocation ref= new  SyncBusinessLocation();
        // ref.writeToCSV(new String[]{"SalesAccountId", "SupplierId" , "BusinessLocation", "Head quarter address" ,"Supplier address" , "Origin"},false);
        ref.getNonSyncSupplierAddress(1086,11);

    }

}

