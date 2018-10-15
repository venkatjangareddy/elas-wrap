/*
 * To change this license header, choose License Headers in Project Properties.
 * and open the template in the editor.
 */
package com.oceaneering.properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Map;
import spark.utils.IOUtils;
import com.oceaneering.common.Utility;
import com.oceaneering.exceptions.InputException;

/**
 *
 * @author SKashyap
 *
 */
public class WrapperProperties {

    private ObjectMapper mapper;
    private JsonNode node;
    private static WrapperProperties _instance = null;

    private JsonNode configProperties;
    private JsonNode userRoles;
    private boolean isValid = false;

    public String currentIndex = null;

    /**
     * @return the node
     */
    public JsonNode getNode() {
        return this.node;
    }

    /**
     * @param app_name the node to set
     */
    public void setNode(String app_name) {
        if (this.getConfig().has(app_name) != false) {
            this.isValid = true;
            this.node = this.getConfig().get(app_name);
        } else {
            isValid = false;
        }
    }

    public void setUserRoles(JsonNode userRoles) {
        this.userRoles = userRoles;
    }

    /**
     * @return the node
     */
    private JsonNode getConfig() {
        return this.configProperties;
    }

    private JsonNode getUserRoles() {
        return this.userRoles;
    }

    /**
     * @param node the node to set
     */
    private void setConfig(JsonNode config) {
        if (this.configProperties == null) {
            this.configProperties = config;
        }
    }

    public static WrapperProperties Instance(String app_name) {

        try {
            _instance = new WrapperProperties();
        } catch (Exception ex) {
            return null;
        }
        _instance.setNode(app_name);
        return _instance;
    }

    private WrapperProperties() {
        InputStream in = null;
        try {
            if (this.configProperties == null) {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                in = classLoader.getResourceAsStream("application.properties.json");
                StringWriter writer = new StringWriter();
                IOUtils.copy(in, writer);
                String jsonString = writer.toString();
                mapper = new ObjectMapper();
                this.setConfig(mapper.readTree(jsonString));
            }
        } catch (Exception ex) {
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ex) {
                }
            }
        }
    }

    public JsonNode getIndex() {
        return this.getNode().get("index");
    }

    public boolean isExternalApp() {
        return this.getNode().get("authorizeViaMDS").asBoolean();
    }

    public boolean isNestedField(String name) {
        return this.getNode().get("nestedField").has(name);
    }

    public JsonNode getESType() {
        return this.getNode().get("type");
    }

    public JsonNode getClassName() {
        return this.getNode().get("className");
    }

    public JsonNode getRoles() throws IOException {
        return this.getNode().get("roles");
    }

    public String getSignedKey() {
        return this.getNode().get("signatureKey").asText();
    }

    public String getAggsSeparator() {
        return this.getNode().get("agg_separator").asText();
    }

    public int getSessionTimeOut() {
        return this.getNode().has("sessionTimeOut")
                ? this.getNode().get("sessionTimeOut").asInt() : 3600; // default value of 1 hour if no value defined in cofig file
    }

    public int getAggSize() {
        return this.getNode().has("aggSize")
                ? this.getNode().get("aggSize").asInt() : 100;
    }

    public int getAggCount() {
        return this.getNode().has("aggCount")
                ? this.getNode().get("aggCount").asInt() : 100;
    }

    public int getSourceSize() {
        return (this.getNode().has("sourceSize") && this.getNode().get("sourceSize").asInt() > 0)
                ? this.getNode().get("sourceSize").asInt() : 200;
    }

    /**
     * @return the mapper
     */
    public ObjectMapper getMapper() {
        return mapper;
    }

    /**
     * @param mapper the mapper to set
     */
    public void setMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public Map<String, Float> getSearchableFields() throws IOException, InputException {
        String inputString = "*:1.0";
        if (this.getUserPropNode(this.getUserRoles()).has("searchableFields")) {
            inputString = this.getUserPropNode(this.getUserRoles()).get("searchableFields").asText();
        }
        return Utility.convertStringToFloatMap(inputString);
    }

    public JsonNode getUserPropNode(JsonNode userRoles) throws IOException, InputException {
        Integer lowest_level = 99999; // a big number taken for a lowest priority
        JsonNode userNode = null;
        for (int i = 0; i < userRoles.size(); i++) {
            String current_role = userRoles.get(i).asText().toLowerCase();
            if (this.getRoles().has(current_role)) {
                int level = this.getRoles().get(current_role).get("level").asInt();
                if (level < lowest_level) {
                    lowest_level = level;
                    userNode = this.getRoles().get(current_role);
                }
            } else {
                throw new InputException(120);
            }
        }
        return userNode;
    }

    public Map<String, Float> getSearchableLoweredFields() throws IOException, InputException {
        String inputString = "*:1.0";
        if (this.getUserPropNode(this.getUserRoles()).has("loweredFields")) {
            inputString = this.getUserPropNode(this.getUserRoles()).get("loweredFields").asText();
        }
        return Utility.convertStringToFloatMap(inputString);
    }

    public boolean isValidNode() {
        return this.isValid;
    }

}
