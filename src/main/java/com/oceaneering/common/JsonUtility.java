/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.common;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonValue;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author nmakkar
 */
public class JsonUtility {

    public static JsonReader getReader(String str) throws MalformedURLException, IOException {
        InputStream is;
        if (Utility.isUrl(str)) {
            URL url = new URL(str);
            is = url.openStream();
        } else {
            is = IOUtils.toInputStream(str);
        }

        return Json.createReader(is);
    }

    public static JsonReader getReader(InputStream is) throws MalformedURLException, IOException {
        return Json.createReader(is);
    }

    public static JsonObject getJsonObjectFromString(String str) throws IOException {
        return getReader(str).readObject();
    }

    public static JsonArray getJsonArrayFromString(String str) throws IOException {
        return getReader(str).readArray();
    }

    public static JsonObject getJsonObjectFromString(InputStream str) throws IOException {
        return getReader(str).readObject();
    }

    public static JsonArray getJsonArrayFromString(InputStream str) throws IOException {
        return getReader(str).readArray();
    }

    public static JsonObjectBuilder getJsonBuilderInEditMode(String str) throws IOException {
        JsonObject obj = getJsonObjectFromString(str);
        return setBuilder(obj);
    }

    public static JsonObjectBuilder getJsonBuilder() throws IOException {
        return Json.createObjectBuilder();
    }

    private static JsonObjectBuilder setBuilder(JsonObject obj) {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        obj.forEach((key, value) -> addKeyToBuilder(key, value, builder));
        return builder;
    }

    private static void addKeyToBuilder(String key, JsonValue value, JsonObjectBuilder builder) {
        if (value instanceof JsonObject) {
            builder.add(key, setBuilder((JsonObject) value));
        } else if (value instanceof JsonArray) {
            builder.add(key, getArrayBuilder((JsonArray) value));
        } else {
            builder.add(key, value);
        }
    }

    private static JsonArrayBuilder getArrayBuilder(JsonArray array) {
        JsonArrayBuilder builder = Json.createArrayBuilder();
        array.forEach((obj) -> {
            if (obj instanceof JsonObject) {
                builder.add(setBuilder((JsonObject) obj));
            } else {
                builder.add(obj);
            }
        });
        return builder;
    }
}
