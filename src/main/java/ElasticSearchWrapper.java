/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import spark.Spark;
import spark.servlet.SparkApplication;
import java.util.HashMap;
import java.util.Map;
import com.oceaneering.token.Authentication;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.oceaneering.common.JsonUtility;
//import com.oceaneering.common.HTMLUtil;
import au.com.bytecode.opencsv.CSVReader;
import com.thoughtworks.xstream.XStream;
import com.oceaneering.common.Utility;
import com.oceaneering.exceptions.UnAuthorizedAccessException;
import com.oceaneering.exceptions.EsException;
import com.oceaneering.properties.WrapperProperties;
import com.oceaneering.properties.Mappings;
import com.oceaneering.search.ESQuery;
import com.oceaneering.exceptions.EsQueryException;
import com.oceaneering.exceptions.ForbiddenException;
import com.oceaneering.exceptions.InputException;
import com.oceaneering.exceptions.TokenAuthenticationException;
import com.oceaneering.mappings.Settings;
import com.oceaneering.search.EsRequest;
import javax.json.JsonObject;
import com.oceaneering.update.AppInterface;
import com.oceaneering.validations.Validator;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import javax.json.JsonObjectBuilder;
import org.apache.log4j.Logger;
import spark.Response;

/**
 *
 * @author SKashyap
 *
 */
public class ElasticSearchWrapper
        implements SparkApplication {

    final static Logger logger = Logger.getLogger(ElasticSearchWrapper.class);

    public static void main(String[] args) throws IOException {
        new ElasticSearchWrapper().init();
    }

    @Override
    public void init() {
        Spark.get((String) "/", (request, response) -> {
            response.redirect("/sparkServlet/hello");
            return null;
        });

        Spark.post((String) "/revoketoken", (request, response) -> {
            Validator validator = new Validator();
            try {
                boolean validRequest = validator.validateRequest(request, "revoketoken");
                ObjectMapper mapper = new ObjectMapper();
                String arrayBody = validator.getRequestBody();
                JsonNode requestNode = (ObjectNode) mapper.readValue(arrayBody, JsonNode.class);
                if (Utility.deleteExpiredTokens(requestNode.get("access_token").asText())) {
                    this.setJsonContentType(response);
                    return "{\"success\":\"true\"}";
                }
                response.status(500);
                return "{\"success\":\"false\"}";
            } catch (ForbiddenException e) {
                response.status(403);
                return this.setErrorResponse(response, e);
            } catch (InputException e) {
                response.status(400);
                return this.setErrorResponse(response, e);
            } catch (TokenAuthenticationException e) {
                response.status(401);
                return this.setErrorResponse(response, e);
            } catch (Exception e) {
                logger.error("Sorry, something wrong!", e);
                response.status(500);
                return this.setGeneralErrorResponse(response, e);
            }
        });

        Spark.post((String) "/search", (request, response) -> {
            try {
                Validator validator = new Validator();
                boolean validRequest = validator.validateRequest(request, "search");
                ObjectMapper mapper = new ObjectMapper();
                String arrayBody = validator.getRequestBody();
                JsonNode requestNode = (ObjectNode) mapper.readValue(arrayBody, JsonNode.class);
                String app_name = requestNode.get("app_name").asText();
                WrapperProperties properties = WrapperProperties.Instance(app_name);
                properties.setUserRoles(validator.userRoles());
                Map<String, String> map = new HashMap();
                map.put("pretty", "true");
                map.put("filter_path", "hits.hits._id,hits.hits._source,aggregations,hits.total");
                ESQuery esquery = new ESQuery(arrayBody, properties, validator.getSource());
                String query = esquery.get();
                Boolean debug = requestNode.has("debug") ? requestNode.get("debug").asBoolean() : false;
                if (debug) {
                    JsonNode qrynode = (JsonNode) mapper.readValue(query, JsonNode.class);
                    return this.setSuccessResponse(response, qrynode);
                }

                JsonNode esresponse = (new EsRequest(query, properties))
                        .setUrlQueryParams(map)
                        .setEndPoint("_search")
                        .post();
                return this.setSuccessResponse(response, esresponse);
            } catch (ForbiddenException e) {
                response.status(403);
                return this.setErrorResponse(response, e);
            } catch (InputException e) {
                response.status(400);
                return this.setErrorResponse(response, e);
            } catch (TokenAuthenticationException e) {
                response.status(401);
                return this.setErrorResponse(response, e);
            } catch (EsQueryException e) {
                response.status(400);
                return this.setErrorResponse(response, e);
            } catch (Exception e) {
                logger.error("Sorry, something wrong!", e);
                response.status(500);
                return this.setGeneralErrorResponse(response, e);
            }
        });

        Spark.post((String) "/update/:id", (request, response) -> {
            try {
                Validator validator = new Validator();
                boolean validRequest = validator.validateRequest(request, "update");
                JsonNode data = null;
                ObjectMapper mapper = new ObjectMapper();
                String arrayBody = validator.getRequestBody();
                JsonNode requestNode = (ObjectNode) mapper.readValue(arrayBody, JsonNode.class);
                String appName = requestNode.get("app_name").asText();
                WrapperProperties properties = WrapperProperties.Instance(appName);
                String className = properties.getClassName().asText();
                AppInterface clazz = Utility.makeClassNameInstance(className);
                JsonNode updateQuery = clazz
                        .setId(request.params("id"))
                        .setUpdateData(requestNode, properties)
                        .update();
                return this.setSuccessResponse(response, updateQuery);
            } catch (ForbiddenException e) {
                response.status(403);
                return this.setErrorResponse(response, e);
            } catch (InputException e) {
                response.status(400);
                return this.setErrorResponse(response, e);
            } catch (TokenAuthenticationException e) {
                response.status(401);
                return this.setErrorResponse(response, e);
            } catch (Exception e) {
                logger.error("Sorry, something wrong!", e);
                response.status(500);
                return this.setGeneralErrorResponse(response, e);
            }
        });

        Spark.post((String) "/generateAuthenticationToken", (request, response) -> {
            try {
                String body = request.body();
                String app_name = null;
                String username = null;
                Validator validate = new Validator();
                boolean validRequest = validate.validateRequest(request, "token");
                ObjectMapper mapper = new ObjectMapper();
                String arrayBody = validate.getRequestBody();
                Map<String, String> authorized = new HashMap();
                JsonObject requestNode = JsonUtility.getJsonObjectFromString(body);
                WrapperProperties properties = WrapperProperties.Instance(requestNode.getString("app_name"));
                String authToken = (new Authentication(requestNode, properties))
                        .token();
                return this.setSuccessResponse(response, authToken, properties.getSessionTimeOut());
            } catch (InputException e) {
                response.status(400);
                return this.setErrorResponse(response, e);
            } catch (UnAuthorizedAccessException e) {
                response.status(400);
                return this.setErrorResponse(response, e);
            } catch (TokenAuthenticationException e) {
                response.status(401);
                return this.setErrorResponse(response, e);
            }
        });

        Spark.get((String) "/updatemappings", (request, response) -> {
            Mappings map_properties = Mappings.Instance();
            JsonObject reader = map_properties.getConfig();
            Settings set = new Settings();
            boolean refresh = request.queryParams().contains("refresh");
            boolean pushData = request.queryParams().contains("add_data");
            Date date = Calendar.getInstance().getTime();
            DateFormat dateFormat = new SimpleDateFormat("dd-MM-YYYY");
            String strDate = dateFormat.format(date);
            System.out.print(strDate);
            if (!request.queryParams().contains("authcode") || !request.queryParams("authcode")
                    .equalsIgnoreCase("difncmmeddfoifncfno34hdej389nj")) {
                return this.setError("Authorization code is required to perform this operation.");
            }
            String index = request.queryParams().contains("index") ? request.queryParams("index") : null;
            JsonObjectBuilder res = set.update(refresh, index, pushData);
            return this.setSuccessResponse(response, res.build());
        });

        Spark.get((String) "/info", (request, response) -> {
            Map<String, String> map = new HashMap();

            map.put("pretty", "true");
            String esresponse = (new EsRequest())
                    .setUrlQueryParams(map)
                    .get().toString();
            ObjectMapper mapper = new ObjectMapper();
            Object jsonObject = mapper.readValue(esresponse, Object.class);
            String prettyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
            if (request.queryParams().contains("pretty")) {
                return Utility.makePrettyOutput(prettyJson);
            } else {
                return prettyJson;
            }
        });

        Spark.get((String) "/convert", (request, response) -> {
            String startFile = "/app/elastic/sample.csv";
            String outFile = "/app/elastic/sample.xml";

            try {
                CSVReader reader = new CSVReader(new FileReader(startFile));
                String[] line = null;

                String[] header = reader.readNext();

                List out = new ArrayList();

                while ((line = reader.readNext()) != null) {
                    List<String[]> item = new ArrayList();
                    //List<String[]> item = new ArrayList<String[]>();
                    for (int i = 0; i < header.length; i++) {
                        String[] keyVal = new String[2];
                        String string = header[i];
                        String val = line[i];
                        keyVal[0] = string;
                        keyVal[1] = val;
                        item.add(keyVal);
                    }
                    out.add(item);
                }

                XStream xstream = new XStream();

                xstream.toXML(out, new FileWriter(outFile, false));

            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return "File Converted to XML!";
        });

        Spark.get((String) "/stats", (request, response) -> {
            String html = "<html><body><div><div id=\"1\" style=\"background-color:lightblue\">Index 1</div><div id=\"2\" style=\"background-color:lightblue\">Index2</div><div id=\"3\" style=\"background-color:lightblue\">Index 3</div></div></body></html>";
            return html;
//            Map<String, String> map = new HashMap();
//            map.put("pretty", "true");
//            Gson gson = new GsonBuilder().setPrettyPrinting().create();
//            JsonParser parser = new JsonParser();
//            String esresponse = (new EsRequest()
//                    .setUrlQueryParams(map)
//                    .setEndPoint("_stats")
//                    .setUrlQueryParams(map)
//                    .get().toString());
//            ObjectMapper mapper = new ObjectMapper();
//            Object jsonObject = mapper.readValue(esresponse, Object.class);
//            String prettyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
//            if (request.queryParams().contains("pretty")) {
//                return Utility.makePrettyOutput(prettyJson);
//            } else {
//                return prettyJson;
//            }
        });

        Spark.options(
                "/*",
                (request, response) -> {

                    String accessControlRequestHeaders = request
                            .headers("Access-Control-Request-Headers");
                    if (accessControlRequestHeaders != null) {
                        response.header("Access-Control-Allow-Headers",
                                accessControlRequestHeaders);
                    }

                    String accessControlRequestMethod = request
                            .headers("Access-Control-Request-Method");
                    if (accessControlRequestMethod != null) {
                        response.header("Access-Control-Allow-Methods",
                                accessControlRequestMethod);
                    }
                    return "OK";
                }
        );

        Spark.before(
                (request, response) -> {
                    response.header("Access-Control-Allow-Origin", "*");
                });

        /*
        API to be used for test the instance status
        - SUCCESS if msg displayed
        - FAILED if no msg displayed
         */
//        Spark.get((String) "/home", (request, response) -> {
//            String html = HTMLUtil.homeContent();
//            return html;
//        });
        Spark.stop();
    }

    private String setError(String message) {
        return "{\"success\":\"false\",\"errorMessage\":" + message + "}";
    }

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

    private JsonNode setSuccessResponse(Response response, JsonObject data) throws IOException {
        this.setJsonContentType(response);
        String jsonString = "{\"success\":\"true\",\"data\":" + data.toString() + "}";
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(jsonString);

    }

    private JsonNode setSuccessResponse(Response response, String data, Integer expiryTime) throws IOException {
        this.setJsonContentType(response);
        String jsonString = "{\"success\":\"true\",\"timeout\":\"" + expiryTime + "\",\"access_token\":\"" + data + "\"}";
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
