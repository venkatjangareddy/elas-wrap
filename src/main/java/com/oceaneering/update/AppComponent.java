/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oceaneering.common.JsonUtility;
import com.oceaneering.properties.WrapperProperties;
import com.oceaneering.search.EsRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;

/**
 *
 * @author SKashyap
 *
 */
public abstract class AppComponent implements AppInterface {

    private JsonNode document = null;
    private JsonObject scriptBody = null;

    private String endpoint = "_search";
    private String method = "GET";
    protected String id;
    protected JsonObject oldEntity = null;
    private WrapperProperties props;

    public AppComponent setId(String id) {
        this.id = id;
        return this;
    }

    public String getId() {
        return this.id;
    }

    @Override
    public AppComponent setUpdateData(JsonNode body, WrapperProperties properties) throws IOException {
        //testing elastic POST
        this.document = body.get("document");
        this.props = properties;
        try {
            this.prepareJsonDocument();
        } catch (IOException ex) {
//                Logger.getLogger(AppComponent.class.getName()).log(Level.SEVERE, null, ex);
        }
        return this;
    }

    public AppComponent prepareJsonDocument() throws IOException {
        JsonObject reader = JsonUtility.getJsonObjectFromString(this.document.toString());
        StringBuilder sb = new StringBuilder();
        JsonObjectBuilder params = Json.createObjectBuilder();
        reader.forEach((key, value) -> {
            if (value.getValueType().toString().equalsIgnoreCase("array")) {
                sb.append("ctx._source.");
                sb.append(key);
                sb.append("=params.");
                sb.append(key);
                sb.append(";");

                try {
                    params.add(key, this.makeDocumentForNestedObject(key, value, false));
                } catch (IOException ex) {
//                Logger.getLogger(AppComponent.class.getName()).log(Level.SEVERE, null, ex);
                }

            } else {
                if (value.getValueType().toString().equalsIgnoreCase("object")) {
                    JsonObject objval = (JsonObject) value;

                    if (objval.containsKey("delete") && objval.get("delete").toString() == "true") {
                        sb.append("ctx._source.").append("remove('").append(key).append("');");
                    } else {
                        sb.append("ctx._source.").append(key).append("=").append("params.").append(key).append(";");

                        try {
                            params.add(key, this.makeDocumentForNestedObject(key, value, true));
                        } catch (IOException ex) {
//                        Logger.getLogger(AppComponent.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                } else {
                    sb.append("ctx._source.").append(key).append("='").append(value).append("';");
                }
            }

        });

        JsonObjectBuilder script_string_builder = Json.createObjectBuilder();
        JsonObjectBuilder script_object_builder = Json.createObjectBuilder();
        script_string_builder.add("source", sb.toString());
        script_string_builder.add("params", params);

        this.scriptBody = script_object_builder.add("script", script_string_builder.build()).build();
        return this;
    }

    private JsonValue makeDocumentForNestedObject(String key, JsonValue data, boolean isObject) throws IOException {
        JsonObject entity = this.getOldEntity();
        ObjectMapper mapper = new ObjectMapper();
        JsonArray merged;
        if (isObject) {
            return data;
        } else {

            JsonArray newval = JsonUtility.getJsonArrayFromString(data.toString());
            boolean isArrayOfObjects = newval.get(0).getValueType().toString().equalsIgnoreCase("object");

            if (isArrayOfObjects) {
                Map<String, JsonObject> oldvalues;
                if (entity.containsKey(key)) {
                    JsonArray olddata = (JsonArray) entity.get(key);
                    if (olddata.size() == 1
                            && (olddata.getJsonObject(0).containsKey("id")
                            && (olddata.getJsonObject(0).get("id").toString().length() == 0)
                            || olddata.getJsonObject(0).get("id").toString().equals("\"\""))) {
                        oldvalues = new HashMap();
                    } else {
                        oldvalues = convertArrayToObject((JsonArray) entity.get(key));
                    }
                } else {
                    oldvalues = new HashMap();
                }

                Map<String, JsonObject> newvalues = convertArrayToObject(newval);
                merged = this.compareAndUpdateObjectArrays(newvalues, oldvalues);
            } else {
                JsonArray oldvalues;
                oldvalues = JsonUtility.getJsonArrayFromString(entity.containsKey(key) ? entity.get(key).toString() : "[]");
                if (oldvalues.isEmpty()) {
                    merged = newval;
                } else {
                    newval.forEach((value) -> {
                        if ((oldvalues.contains(value))) {
                            oldvalues.add((JsonValue) value);
                        }

                    });
                    merged = oldvalues;
                }

            }
            System.out.println(merged.toString());
            return merged;
        }
    }

    private Map<String, JsonObject> convertArrayToObject(JsonArray nodearray) {
        Map<String, JsonObject> map = new HashMap();

        int i = 10;
        for (JsonObject obj : nodearray.getValuesAs(JsonObject.class)) {
            if (obj.containsKey("id")) {
                map.put(obj.get("id").toString(), obj);
            } else {
                map.put(Integer.toString(i++), obj);
            }
        };
        return map;
    }

    private JsonArray compareAndUpdateObjectArrays(Map<String, JsonObject> newdata, Map<String, JsonObject> oldata) throws IOException {
        newdata.forEach((id, node) -> {
            if (node.containsKey("delete") && node.get("delete").toString() == "true") {
                oldata.put(id, null);
            } else {
                if (oldata.containsKey(id)) {
                    node = mergeNode(oldata.get(id), node);
                }
                oldata.put(id, node);

            }
        });
        JsonArrayBuilder merged = Json.createArrayBuilder();
        oldata.forEach((key, val) -> {
            if (val != null) {
                merged.add(val);
            }
        });
        return merged.build();
    }

    private JsonObject mergeNode(JsonObject oldNode, JsonObject newNode) {
        Map<String, JsonObject> map = new HashMap();
        JsonObjectBuilder merged = Json.createObjectBuilder();
        oldNode.forEach((key, val) -> {
            if (val != null) {
                merged.add(key, val);
            }
        });
        newNode.forEach((key, value) -> {
            merged.add(key, value);

        });

        return merged.build();
    }

    public JsonObject getOldEntity() throws IOException {
        if (this.oldEntity == null) {
            NStringEntity entity = new NStringEntity("", ContentType.APPLICATION_JSON);
            JsonNode esResponse = (new EsRequest(entity.toString(), props))
                    .setEndPoint(props.getESType().asText() + "/" + this.getId())
                    .debug()
                    .get();
            if (esResponse.has("_source")) {
                this.oldEntity = JsonUtility.getJsonObjectFromString(esResponse.get("_source").toString());
            } else {
                this.oldEntity = JsonUtility.getJsonObjectFromString("{}");
            }
        }
        return this.oldEntity;
    }

    public JsonNode update() throws IOException {
        Map<String, String> map = new HashMap();
        return (new EsRequest(this.scriptBody.toString(), this.props))
                .setUrlQueryParams(map)
                .setEndPoint(this.props.getESType().asText() + "/" + this.getId() + "/_update")
                .post();
    }
}
