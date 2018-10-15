/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.validations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.oceaneering.common.JsonUtility;
import com.oceaneering.exceptions.ForbiddenException;
import com.oceaneering.exceptions.InputException;
import com.oceaneering.exceptions.TokenAuthenticationException;
import com.oceaneering.properties.WrapperProperties;
import com.oceaneering.token.Authentication;
import java.io.IOException;
import java.util.Map;
import javax.json.JsonArray;
import javax.json.JsonObject;
import spark.Request;

/**
 *
 * @author SKashyap
 *
 */
public class Validator {

    private boolean isValid = true;
    private String requestBody = null;
    public boolean isJSONValid;
    public WrapperProperties properties;
    private JsonNode userRoles;

    public boolean validateRequest(Request request, String type) throws IOException, InputException, ForbiddenException, TokenAuthenticationException {
        String node = request.body();
        this.validateCommonRequestChecks(node, type);
        switch (type) {
            case "search":
                this.validateSearchRequest(node);
                break;
            case "update":
                this.validateUpdateRequest(node);
                break;
            case "token":
                this.validateTokenGeneratorRequest(node);
                break;
        }
        return this.isValid;
    }

    public JsonNode userRoles() {
        return this.userRoles;
    }

    public boolean validateSearchRequest(String node) throws InputException, IOException, TokenAuthenticationException, ForbiddenException {
        this.validateSearchSource();
        return this.isValid;
    }

    private boolean validateUpdateRequest(String node) throws InputException, TokenAuthenticationException, IOException {
//        JsonObject reader = JsonUtility.getJsonObjectFromString(node).getJsonObject("document");
//        for (int i = 0; i < reader.keySet().size(); i++) {
//            if (!reader.containsKey("id")) {
//                throw new InputException(119);
//            }
//        }
//        JsonObject document = reader.getJsonObject("document");
//        for (JsonObject obj : doc.g) {
//
//        }
//        System.out.println("jddj");
//        System.out.println(doc.keySet().toString());
//        JsonArray array = doc.getJsonArray(doc.toString());
//        for (JsonObject obj : array.getValuesAs(JsonObject.class)) {
//            if (!obj.containsKey("id")) {
//                throw new InputException(119);
//            }
//        }
        return this.isValid;
    }

    public boolean validateTokenGeneratorRequest(String node) throws InputException, IOException {
        return this.isValid;
    }

    private boolean validateCommonRequestChecks(String node, String type) throws InputException, TokenAuthenticationException {
        try {
            if (node.length() == 0) {
                throw new InputException(117);
            }
            JsonParser parser = new JsonParser();
            this.requestBody = parser.parse(node).toString();
        } catch (JsonSyntaxException jse) {
            this.isValid = false;
            throw new InputException(109);
        } catch (Exception e) {
            System.out.println(101);
        }

        try {
            String body = node;
            if (isValid) {
                String tokenAndUserInfo = null;
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode arrayBody;
                try {
                    arrayBody = (ObjectNode) mapper.readValue(body, JsonNode.class);
                } catch (IOException ex) {
                    isValid = false;
                    throw new InputException(101);
                }
                String app_name = null;
                if (arrayBody.has("app_name")) {
                    app_name = arrayBody.get("app_name").asText();
                    isValid = true;
                } else {
                    isValid = false;
                    throw new InputException(102);
                }
                this.properties = WrapperProperties.Instance(app_name);
                if (!this.properties.isValidNode()) {
                    isValid = false;
                    throw new InputException(104);
                }
                if (!type.equalsIgnoreCase("token")) {
                    if (!arrayBody.has("access_token")) {
                        isValid = false;
                        throw new InputException(105);
                    }
                    tokenAndUserInfo = arrayBody.get("access_token").asText();
                    Map<String, String> tokenbody = Authentication.validateToken(tokenAndUserInfo, properties.getSignedKey());

                    String userRoles_string = tokenbody.get("roles").toString();
                    ObjectMapper rolemap = new ObjectMapper();
                    this.userRoles = rolemap.readTree(userRoles_string); // invalid user roles test
                }
            }
        } catch (IOException ex) {
            isValid = false;
            throw new InputException(112);
        }
        return isValid;
    }

    public String getRequestBody() {
        return this.requestBody;
    }

    public boolean validateSearchSource() throws IOException, ForbiddenException, InputException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode requestBody = (ObjectNode) mapper.readValue(this.requestBody, JsonNode.class);
        if (requestBody.has("aggs")) {
            String accessSource = getSource().toString();
            String name;
            for (int i = 0; i < requestBody.get("aggs").size(); i++) {
                if (requestBody.get("aggs").get(i).get("agg_field").isArray()) {
                    JsonNode fields = requestBody.get("aggs").get(i).get("agg_field");
                    for (int j = 0; j < fields.size(); j++) {
                        name = fields.get(j).asText();
                        if (!(accessSource.equals("[]")) && !accessSource.contains("\"" + name + "\"")) {
                            this.isValid = false;
                            throw new ForbiddenException(111);
                        }
                    }
                } else {
                    name = requestBody.get("aggs").get(i).get("agg_field").asText();
                    if (!(accessSource.equals("[]")) && !accessSource.contains("\"" + name + "\"")) {
                        this.isValid = false;
                        throw new ForbiddenException(111);
                    }
                }
            }
        }
        return this.isValid;
    }

    public JsonNode getSource() throws IOException, InputException {
        return properties.getUserPropNode(userRoles).get("source");
    }

}
