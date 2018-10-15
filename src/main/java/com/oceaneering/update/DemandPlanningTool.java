/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.update;

import com.fasterxml.jackson.databind.JsonNode;

/**
 *
 * @author SKashyap
 *
 */
public class DemandPlanningTool extends AppComponent {

    private String caller = "PUT";

    @Override
    public DemandPlanningTool setId(String id) {
        if (id == null) {
            id = "99999"; // we will define the exact logic later
        }

        this.id = id;
        return this;
    }

    @Override
    public JsonNode update() {
        return null;
    }

    public JsonNode post() {
        return null;
    }
}
