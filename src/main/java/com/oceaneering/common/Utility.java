/*
 * To change this license header, choose License Headers in Project Properties.
 * and open the template in the editor.
 */
package com.oceaneering.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.oceaneering.exceptions.EsException;
import com.oceaneering.exceptions.EsQueryException;
import com.oceaneering.exceptions.InputException;
import com.oceaneering.exceptions.TokenAuthenticationException;
import com.oceaneering.properties.WrapperProperties;
import com.oceaneering.search.ESQuery;
import com.oceaneering.search.EsRequest;
import com.oceaneering.token.Authentication;
import com.oceaneering.update.AppInterface;
import com.oceaneering.validations.Validator;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import javax.json.JsonObjectBuilder;
import spark.Response;

/**
 *
 * @author SKashyap
 *
 */
public class Utility {

    public static ObjectMapper mapper = null;

    public static String[] split(String text) {
        String[] arr = text.split("-|_|,|\\s|\\/|:");
        return arr;
    }

    public static Object makePrettyOutput(Object prettify) {
        return "<html><head><body><pre>" + prettify + "</pre></body>";
    }

    public static Map<String, Float> convertStringToFloatMap(String str) {
        return (HashMap<String, Float>) Arrays.asList(str.split(","))
                .stream().map(s -> s.split(":"))
                .collect(Collectors.toMap(e -> e[0], e -> Float.parseFloat(e[1])));
    }

    public static Map<String, Integer> convertStringToIntMap(String str) {
        return (HashMap<String, Integer>) Arrays.asList(str.split(",")).stream().map(s -> s.split(":")).collect(Collectors.toMap(e -> e[0], e -> Integer.parseInt(e[1])));
    }

    public static Map<String, String> convertStringToMap(String str) {
        return (HashMap<String, String>) Arrays.asList(str.split(",")).stream().map(s -> s.split(":")).collect(Collectors.toMap(e -> e[0], e -> e[1]));
    }

    public static AppInterface makeClassNameInstance(String name) {
        try {
            String appName = "com.oceaneering.update." + capitalize(name);
            Class clazz = Class.forName(appName);
            return (AppInterface) clazz.newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean isNestedField(String fieldName, WrapperProperties wrapper) {
        return wrapper.isNestedField(fieldName);
    }

    public static String capitalize(String name) {
        if (name == null || name.length() == 0) {
            return name;
        }
        char chars[] = name.toCharArray();
        chars[0] = Character.toUpperCase(chars[0]);
        return new String(chars);
    }

    public static boolean isUrl(String s) {
        String regex = "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        try {
            Pattern patt = Pattern.compile(regex);
            Matcher matcher = patt.matcher(s);
            return matcher.matches();
        } catch (RuntimeException e) {
            return false;
        }
    }

    public static boolean checkInputForSearchKeyword(String key) {
        Integer keyword = null;
        boolean result;
        try {
            keyword = Integer.parseInt(key);
            result = true;
        } catch (NumberFormatException e) {
            result = false;
        }
        return result;
    }

    public static boolean isTokenExists(String token) throws IOException {
        WrapperProperties jwtProps = WrapperProperties.Instance("JWT Expiry");
        JsonNode tokenStats = (new EsRequest(token, jwtProps))
                .setEndPoint(jwtProps.getESType().asText() + "/" + token)
                .get();
        if (!tokenStats.toString().contains("Not Found")) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean deleteExpiredTokens(String expToken) throws IOException {
        WrapperProperties jwtProps = WrapperProperties.Instance("JWT Expiry");
        System.out.println(jwtProps.getESType().asText() + "/" + expToken);
        JsonNode tokenStats = (new EsRequest(expToken, jwtProps))
                .setEndPoint(jwtProps.getESType().asText() + "/" + expToken)
                .delete();
        if (tokenStats.toString().contains("deleted")) {
            return true;
        } else {
            return false;
        }
    }

//    public static void main(String... args) throws IOException, InputException, TokenAuthenticationException {
//        boolean isValid = false;
//        String body = "{\n" +
//"   \"search_keyword\": \"hari\",\n" +
//"   \"page_size\": 24,\n" +
//"   \"access_token\": \"eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJQZW9wbGVGaW5kZXIiLCJleHAiOjE1MzMyODU2NzQsInVzZXJuYW1lIjoiZGtob3NsYSIsImFwcF9uYW1lIjoiUGVvcGxlRmluZGVyIiwic2VydmVyTmFtZSI6IkRFViIsInJvbGVzIjoiW1wiYWRtaW5cIl0ifQ.Vb8Bvk_8H_iIw4ne17HVemUeECvipcV_RjfHT6rHMyo\",\n" +
//"   \"app_name\": \"PeopleFinder\",\n" +
//"   \"filter\":\n" +
//" {\"empjob\":\"dasfsd\",\"filter\":{\"empcountry\":[\"QAT\",\"SGP\",\"IND\"],\"empcity\":[\"Chandigarh\"],\"operator\":\"or\"}}}\n" +
//" }";
//System.out.println(body);        
//// pf
////        String body = "{\"success\":\"true\",\"timeout\":\"3600\",\"access_token\":\"eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJEZW1hbmQgUGxhbm5pbmcgVG9vbCIsImV4cCI6MTUzMjMzNzA5MSwidXNlcm5hbWUiOiJka2hvc2xhIiwiYXBwX25hbWUiOiJEZW1hbmQgUGxhbm5pbmcgVG9vbCIsInNlcnZlck5hbWUiOiJERVYiLCJyb2xlcyI6IltcIkFETUlOXCJdIn0.xXgT4Z43-wVlpEIS-tJy4oBgFyPPjignl1cwzanzwig\"}";
//        String tokenAndUserInfo = null;
//                ObjectMapper mapper = new ObjectMapper();
//                ObjectNode arrayBody;
//                try {
//                    arrayBody = (ObjectNode) mapper.readValue(body, JsonNode.class);
//                } catch (IOException ex) {
//                    isValid = false;
//                    throw new InputException(101);
//                }
//                               
//                
//                    if (!arrayBody.has("access_token")) {
//                        isValid = false;
//                        throw new InputException(105);
//                    }
//                    tokenAndUserInfo = arrayBody.get("access_token").asText();
//                    Map<String, String> tokenbody = Authentication.validateToken(tokenAndUserInfo, "kdjfdjfkdfjfr48rhdjkhdfjkdf4r49dkdkfkfkfhuincmn"); // pf
////                    Map<String, String> tokenbody = Authentication.validateToken(tokenAndUserInfo, "hdiwnb83kcls8emg9w6yermq87elw2k492643mh72l28der");
//                    
//                    String userRoles_string = tokenbody.get("roles");
//                    ObjectMapper rolemap = new ObjectMapper();
//                    JsonNode userRoles = rolemap.readTree(userRoles_string); // invalid user roles test
////                    WrapperProperties properties = WrapperProperties.Instance("Demand Planning Tool");
//                    WrapperProperties properties = WrapperProperties.Instance("PeopleFinder");
//                    Integer lowest_level = 99999; // a big number taken for a lowest priority
//        JsonNode source = null;
//        for (int i = 0; i < userRoles.size(); i++) {
//            String current_role = userRoles.get(i).asText().toLowerCase();
//            int level = properties.getRoles().get(current_role).get("level").asInt();
//            if (level < lowest_level) {
//                lowest_level = level;
//                source = properties.getRoles().get(current_role).get("source");
//            }
//        }
//        
//                
//    }
    public static void main__(String... args) throws IOException {
        JsonObjectBuilder builder = JsonUtility.getJsonBuilder();
        builder.add("Java version", System.getProperty("java.version"));
        String esresponse = (new EsRequest())
                .get().toString();
        builder.add("ES version", esresponse);
//            Runtime rt = Runtime.getRuntime();
//            try {
//                Process pr = rt.exec("curl -XGET 'localhost:9200'");
//                String esinfo = pr.getOutputStream().toString();
//                builder.add("ElasticSearch info", esinfo);
//            } catch (IOException ex) {
//                java.util.logging.Logger.getLogger(ElasticSearchWrapper.class.getName()).log(Level.SEVERE, null, ex);
//            }
        System.out.println(builder.build().toString());
    }

    public static void main(String... args) throws IOException {

        String arrayBody = "{\n"
                + "   \"search_keyword\": "+"\"\"hari\"\""+",\n"
                + "   \"page_size\": 24,\n"
                + "   \"access_token\": \"eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJQZW9wbGVGaW5kZXIiLCJleHAiOjE1MzQ0MjU4NDksInVzZXJuYW1lIjoiZGd1cHRhIiwiYXBwX25hbWUiOiJQZW9wbGVGaW5kZXIiLCJzZXJ2ZXJOYW1lIjoiREVWIiwicm9sZXMiOiJbXCJhZG1pblwiXSJ9.pVMraEDavLqMW3Cj2nc0QwsSPS1MGj0q0MwFd6rgIqY\",\n"
                + "   \"app_name\": \"PeopleFinder\",\n"
                + "   \"filter\":\n"
                + " {\"empjob\":\"dasfsd\",\"filter\":{\"empcountry\":[\"QAT\",\"SGP\",\"IND\"],\"empcity\":[\"Chandigarh\"],\"operator\":\"or\"}}}\n"
                + " }";
//        System.out.println(arrayBody);
        Validator validator = new Validator();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode requestNode = (ObjectNode) mapper.readValue(arrayBody, JsonNode.class);
        String app_name = requestNode.get("app_name").asText();
        WrapperProperties properties = WrapperProperties.Instance(app_name);
        ObjectMapper rolemap = new ObjectMapper();
        JsonNode userRoles = rolemap.readTree("[\"admin\"]");

        properties.setUserRoles(userRoles);
        Map<String, String> map = new HashMap();
        map.put("pretty", "true");
        map.put("filter_path", "hits.hits._id,hits.hits._source,aggregations");
        ESQuery esquery = new ESQuery(arrayBody, properties, rolemap.readTree("[]"));
        try {
            String query = esquery.get();
            System.out.println((JsonNode) mapper.readValue(query, JsonNode.class));
        } catch (EsQueryException ex) {
            Logger.getLogger(Utility.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InputException ex) {
            Logger.getLogger(Utility.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    public String type = null;

//    public static void main(String... args) throws IOException {
//        Mappings map_properties = Mappings.Instance();
//        JsonObject reader = map_properties.getConfig();
//
//        reader.forEach((index, index_settings) -> {
//            Map<String, String> map = new HashMap();
//            JsonObject settings_reader;
//            try {
//                settings_reader = JsonUtility.getJsonObjectFromString(index_settings.toString());
//                JsonObject mappings = settings_reader.getJsonObject("mappings");
//                JsonObject settings = settings_reader.getJsonObject("settings");
//                String type = mappings.getString("type");
//                JsonObject mapping = mappings.getJsonObject("mapping");
//                String response = (new EsRequest())
//                        .setIndex(index)
//                        .setUrlQueryParams(map)
//                        .head();
//                if (response.equalsIgnoreCase("HTTP/1.1 404 Not Found")) {
//                    // index does not exist so lets just create it first
//                    (new EsRequest())
//                            .setIndex(index)
//                            .setUrlQueryParams(map)
//                            .put();
//                }
//
//                (new EsRequest(mapping.toString()))
//                        .setIndex(index)
//                        .setUrlQueryParams(map)
//                        .setEndPoint("_mapping/" + type)
//                        .put();
//
//            } catch (IOException ex) {
//                Logger.getLogger(Utility.class.getName()).log(Level.SEVERE, null, ex);
//            }
//
//        });
//
//    }
//    public static void main(String... args) throws IOException {
//        //getOldEntity();
//        String arrayBody = " { \n"
//                + "\"app_name\": \"PeopleFinder\", \n"
//                + "\"access_token\":\"eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJQZW9wbGVGaW5kZXIiLCJleHAiOjE1MjY5ODQ1MDAsInVzZXJuYW1lIjoiZGtob3NsYSIsInVzZXJJbmZvIjp7ImVtcElkIjoiMDIwMTI4MjQiLCJ1c2VybmFtZSI6ImRraG9zbGEifX0.n_6gyIvGqw-4Yi3O0JjM3vno9YLw73FcRt-iTCjdPbM\",\n"
//                + "\"document\": {\"empbusinessline\":\"This is  dummy businessline for test\",\"empaboutme\":[ { \"aboutme\": \"open source new\",\"aboutmehasorder\": \"3\",\"id\": \"1\"}]}\n"
//                + "\n"
//                + "}";
//
//        ObjectMapper mapper = new ObjectMapper();
//        JsonNode requestNode = (ObjectNode) mapper.readValue(arrayBody, JsonNode.class);
//        String appName = requestNode.get("app_name").asText();
//        WrapperProperties properties = WrapperProperties.Instance(appName);
//        String className = properties.getClassName().asText();
//        AppInterface clazz = Utility.makeClassNameInstance(className);
//        JsonNode updateQuery = clazz
//                .setId("29891")
//                .setUpdateData(requestNode, properties)
//                .update();
//        System.out.println(updateQuery);
//
//    }
    private JsonNode setErrorResponse(Response response, EsException e) throws IOException {
        this.setJsonContentType(response);
        String jsonString = "{\"success\":\"false\",\"errorMessage\":" + e.getLocalizedMessage() + ",\"errorCode\":" + e.getCode() + "}";
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(jsonString);
    }

    private JsonNode setGeneralErrorResponse(Response response, Exception e) throws IOException {
        this.setJsonContentType(response);
        String jsonString = "{\"success\":\"false\",\"errorMessage\":" + e.getLocalizedMessage() + ",\"errorCode\":500}";
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(jsonString);
    }

    private JsonNode setSuccessResponse(Response response, JsonNode data) throws IOException {
        this.setJsonContentType(response);
        String jsonString = "{\"success\":\"true\",\"data\":" + data.toString() + "}";
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(jsonString);
    }

    private void setJsonContentType(Response response) {
        response.header("ContentType", "Application/Json");
    }

    public static void setCurrentIndex(String name, WrapperProperties properties) {
        if (name.isEmpty() == false && name.length() > 0) {
            properties.currentIndex = name;
        } else {
        }
    }

}
