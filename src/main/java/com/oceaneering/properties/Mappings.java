/*
 * To change this license header, choose License Headers in Project Properties.
 * and open the template in the editor.
 */
package com.oceaneering.properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oceaneering.common.JsonUtility;
import java.io.IOException;
import java.io.InputStream;
import javax.json.JsonObject;

/**
 * 
 * @author SKashyap
 *
 */
public class Mappings {

    private JsonObject configProperties;


    /**
     * @return the node
     */
    public JsonObject getConfig() {
        return this.configProperties;
    }

    /**
     * @param node the node to set
     */
    private void setConfig(JsonObject config) {
        if (this.configProperties == null) {
            this.configProperties = config;
        }
    }

    public static Mappings Instance() {

        try {
            _instance = new Mappings();
        } catch (Exception ex) {
            return null;
        }
        return _instance;
    }

    private Mappings() {
        InputStream in = null;
        try {
            if (this.configProperties == null) {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                in = classLoader.getResourceAsStream("mappings.json");
                JsonObject jr = JsonUtility.getJsonObjectFromString(in);
                this.setConfig(jr);
//                StringWriter writer = new StringWriter();
//                IOUtils.copy(in, writer);
//                String jsonString = writer.toString();
//                mapper = new ObjectMapper();
//                this.setConfig(mapper.readTree(jsonString));
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

    private ObjectMapper mapper;
    private static Mappings _instance = null;
}
