/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.mappings;

import com.oceaneering.common.JsonUtility;
import com.oceaneering.common.Utility;
import com.oceaneering.properties.Mappings;
import com.oceaneering.search.EsRequest;
import java.io.IOException;
import static java.lang.Thread.sleep;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

/**
 *
 * @author SKashyap
 */
public class Settings {

    JsonObjectBuilder res = null;

    public JsonObjectBuilder update(Boolean refresh, String indexName, Boolean pushData) throws IOException, InterruptedException {
        Mappings map_properties = Mappings.Instance();
        JsonObject reader = map_properties.getConfig();
        res = JsonUtility.getJsonBuilder();

        try {
            reader.forEach((index, index_settings) -> {

                if (!indexName.equals(null)) {
                    List<String> list = Arrays.asList(indexName.split(","));
                    if (list.contains(index)) {
                        try {
                            res = updateMappings(index, index_settings, refresh, pushData);
                        } catch (IOException ex) {
                            Logger.getLogger(Settings.class.getName()).log(Level.SEVERE, null, ex);
                        } catch (InterruptedException ex) {
                            Logger.getLogger(Settings.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                } else {
                    try {
                        res = updateMappings(index, index_settings, refresh, pushData);
                    } catch (IOException ex) {
                        Logger.getLogger(Settings.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(Settings.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            });
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(Utility.class.getName()).log(Level.SEVERE, null, ex);
        }

        return res;
    }

    public JsonObjectBuilder updateMappings(String index, JsonValue index_settings, boolean refresh, boolean pushData) throws IOException, InterruptedException {

        Map<String, String> map = new HashMap();
        JsonObject settings_reader = null;
        JsonObjectBuilder resp = JsonUtility.getJsonBuilder();
        try {
            settings_reader = JsonUtility.getJsonObjectFromString(index_settings.toString());
            boolean mapping_exists = false;
            boolean setting_exists = false;
            if (settings_reader.containsKey("mappings")) {
                mapping_exists = true;
            }
            if (settings_reader.containsKey("settings")) {
                setting_exists = true;
            }
            if ((mapping_exists || setting_exists)) {
                if (refresh) {
                    // if refresh key comes in request, means delete the index in case it exists
                    String esresponse = (new EsRequest())
                            .setIndex(index)
                            .setUrlQueryParams(map)
                            .head();
                    resp.add("index exists" + index, true);
                    sleep(1);
                    if (esresponse.equals("HTTP/1.1 200 OK")) {
                        (new EsRequest())
                                .setIndex(index)
                                .setUrlQueryParams(map)
                                .delete();
                        resp.add("deleted index " + index, true);
                        sleep(1);
                    }
                }
                // check if index exists
                String esresponse = (new EsRequest())
                        .setIndex(index)
                        .setUrlQueryParams(map)
                        .head();
                resp.add("index exists " + index, true);
                sleep(1);
                // if index does not exists, then create a new one
                if (esresponse.equalsIgnoreCase("HTTP/1.1 404 Not Found")) {
                    // index does not exist so lets just create it first
                    (new EsRequest())
                            .setIndex(index)
                            .setUrlQueryParams(map)
                            .put();
                    resp.add("creating new index " + index, true);
                    sleep(1);
                }

                if (setting_exists) {
                    JsonObject settings = settings_reader.getJsonObject("settings");
                    // close the index
                    (new EsRequest())
                            .setIndex(index)
                            .setEndPoint("_close")
                            .post();

                    resp.add("index closed " + index, true);
                    sleep(1);
                    // update the settings first
                    (new EsRequest(settings.toString()))
                            .setIndex(index)
                            .setUrlQueryParams(map)
                            .setEndPoint("_settings")
                            .put();
                    resp.add("settings updated", true);
                    sleep(1);

                    // open the index now 
                    (new EsRequest())
                            .setIndex(index)
                            .setEndPoint("_open")
                            .post();
                    resp.add("index reopened " + index, true);
                    sleep(1);
                }
                // update the mappings now
                if (mapping_exists) {
                    JsonObject mappings = settings_reader.getJsonObject("mappings");
                    String type = mappings.getString("type");
                    JsonObject mapping = mappings.getJsonObject("mapping");
                    (new EsRequest(mapping.toString()))
                            .setIndex(index)
                            .setUrlQueryParams(map)
                            .setEndPoint("_mapping/" + type)
                            .put();
                    sleep(1);
                    resp.add(index, true);

                    if (pushData) {
                        Runtime rt = Runtime.getRuntime();
                        try {
                            System.out.println("nohup /app/elastic/golang/scripts/syncMongoEs -t /app/elastic/golang/scripts/transform_" + index + "_update.js -n /app/elastic/golang/scripts/pipeline_all.js -i " + index + " -c " + type + " &");
                            Process pr = rt.exec("nohup /app/elastic/golang/scripts/syncMongoEs -t /app/elastic/golang/scripts/transform_" + index + "_update.js -n /app/elastic/golang/scripts/pipeline_all.js -i " + index + " -c " + type + " &");
                        } catch (IOException ex) {
                            java.util.logging.Logger.getLogger(Settings.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(Utility.class.getName()).log(Level.SEVERE, null, ex);
        }

        return resp;
    }

}
