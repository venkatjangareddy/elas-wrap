/*
 * To change this license header, choose License Headers in Project Properties.
 * and open the template in the editor.
 */
package com.oceaneering.errors;

/**
 *
 * @author SKashyap
 *
 */
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import static com.oceaneering.common.Utility.mapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import spark.utils.IOUtils;

public class ErrorCodes {

    private static ErrorCodes _instance = null;
    private JsonNode messages;

    public static ErrorCodes Instance() {
        if (_instance == null) {
            try {
                _instance = new ErrorCodes();
            } catch (Exception ex) {
                return null;
            }
        }
        return _instance;
    }

    private void setNode(JsonNode config) {
        if (this.messages == null) {
            this.messages = config;
        }
    }

    private ErrorCodes() {
        InputStream in = null;
        try {
            if (this.messages == null) {
                //System.out.println("Reading JSON File Properties");
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                in = classLoader.getResourceAsStream("error_code_messages.json");
                StringWriter writer = new StringWriter();
                IOUtils.copy(in, writer);
                String jsonString = writer.toString();
                mapper = new ObjectMapper();
                this.setNode(mapper.readTree(jsonString));
            }
        } catch (Exception ex) {
            //System.out.println("Exception ex" + ex);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ex) {
                }
            }
        }
    }

    public static String getMEssageFromCode(Integer code) {
        JsonNode messages = getErrorMessagesList();
        String codestring = code.toString();
        if (messages.has(codestring)) {
            return messages.get(codestring).toString();
        }
        return "No exception message for code " + codestring; // temp
    }

    private static JsonNode getErrorMessagesList() {
        ErrorCodes properties = ErrorCodes.Instance();
        return properties.messages;
    }
}
