/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.oceaneering.properties.WrapperProperties;
import java.io.IOException;

/**
 *
 * @author SKashyap
 *
 */
public interface AppInterface {

    public AppInterface setId(String id);

    public AppInterface setUpdateData(JsonNode body, WrapperProperties properties) throws IOException;

    public JsonNode update() throws IOException;
}
