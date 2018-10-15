/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.oceaneering.common.Utility;
import com.oceaneering.exceptions.EsQueryException;
import com.oceaneering.properties.WrapperProperties;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

/**
 *
 * @author SKashyap
 *
 */
public class ESFilter {

    private JsonNode filterData;
    private String operator = "and";

    private QueryBuilder filterQuery;
    private WrapperProperties properties;
    private BoolQueryBuilder bool = boolQuery();

    ESFilter(JsonNode data, WrapperProperties properties) {
        this.filterData = data;
        this.properties = properties;
    }

    private ESFilter setData() throws EsQueryException {
        this.filterQuery = this.makeBoolFilter(this.filterData);
        return this;
    }

    private BoolQueryBuilder makeBoolFilter(JsonNode data) throws EsQueryException {
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        boolean addToBuilder;
        BoolQueryBuilder bool = boolQuery();
        try {
            String operator = (data.has("operator") ? data.get("operator").asText() : "and").toLowerCase();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) fields.next();
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                QueryBuilder buildquery = null;
                addToBuilder = true;
                switch (key) {
                    case "gt":
                        buildquery = this.rangeGt(value);
                        break;
                    case "gte":
                        buildquery = this.rangeGte(value);
                        break;
                    case "lt":
                        buildquery = this.rangeLt(value);
                        break;
                    case "lte":
                        buildquery = this.rangeLte(value);
                        break;
                    case "range":
                        buildquery = this.range(value);
                        break;
                    case "not":
                        buildquery = this.mustNot(value);
                        break;
                    case "operator":
                         addToBuilder = false;
                        break;
                    case "nested":
                        buildquery = this.nested(value);
                        break;
                    case "filter":
                        buildquery = this.makeBoolFilter(value);
                        break;
                    default:
                        buildquery = this.termQry(key, value);
                        break;
                }
                if (addToBuilder == true) {
                    if (operator.equalsIgnoreCase("and")) {
                        bool.must(buildquery);
                    } else if (operator.equalsIgnoreCase("or")) {
                        bool.should(buildquery);
                    }
                }
            }

        } catch (Exception e) {
            // write code for deciding the error code
            // expected to be a bad request
            throw new EsQueryException(e.getLocalizedMessage());
        }
        return bool;
    }

    private BoolQueryBuilder rangeGt(JsonNode data) {
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        BoolQueryBuilder bool = boolQuery();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) fields.next();
            String key = entry.getKey();
            Object value = entry.getValue().asText();
            RangeQueryBuilder range = rangeQuery(key);
            range.gt(value);
            bool.must(range);
        }
        return bool;
    }

    private BoolQueryBuilder nested(JsonNode data) throws EsQueryException {
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        String key = null;
        NestedQueryBuilder nested = null;
        BoolQueryBuilder external_bool = boolQuery();
        String operator = "and";
        if (data.has("operator") && data.get("operator").asText().equalsIgnoreCase("or")) {
            operator = "or";
        }
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) fields.next();
            key = entry.getKey();
            if (key == "operator") {
                continue;
            }
            JsonNode value = entry.getValue();

            BoolQueryBuilder bool = this.makeBoolFilter(value);
            nested = nestedQuery(key, bool, ScoreMode.None);
            if (operator == "and") {
                external_bool.must(nested);
            } else if (operator == "or") {
                external_bool.should(nested);
            }
            nested = null;
        }
        return external_bool;
    }

    private BoolQueryBuilder mustNot(JsonNode data) {
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        BoolQueryBuilder bool = boolQuery();
        QueryBuilder query = null;
        while (fields.hasNext()) {

            switch (data.get("criteria").toString()) {
                case "partial":
                    String[] keywords = Utility.split(data.get("value").asText());
                    BoolQueryBuilder search_query = boolQuery();
                    for (int idx = 0; idx < keywords.length; idx++) {
                        String keyword = "*" + keywords[idx] + "*";
                        QueryStringQueryBuilder qr = queryStringQuery(keyword);
                        qr.field(data.get("field").toString());
                        search_query.should(qr);
                    }
                    query = search_query;
                    break;

                case "exact":
                    query = this.termQry(data.get("field").toString(), data.get("value"));
                    break;
            }

            bool.mustNot(query);
        }
        return bool;
    }

    private BoolQueryBuilder rangeGte(JsonNode data) {
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        BoolQueryBuilder bool = boolQuery();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) fields.next();
            String key = entry.getKey();
            Object value = entry.getValue().asText();
            RangeQueryBuilder range = rangeQuery(key);
            range.gte(value);
            bool.must(range);
        }
        return bool;
    }

    private BoolQueryBuilder rangeLt(JsonNode data) {
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        BoolQueryBuilder bool = boolQuery();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) fields.next();
            String key = entry.getKey();
            Object value = entry.getValue().asText();

            RangeQueryBuilder range = rangeQuery(key);
            range.lt(value);
            bool.must(range);
        }

        return bool;
    }

    private BoolQueryBuilder rangeLte(JsonNode data) {
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        BoolQueryBuilder bool = boolQuery();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) fields.next();
            String key = entry.getKey();
            Object value = entry.getValue().asText();
            RangeQueryBuilder range = rangeQuery(key);
            range.lte(value);
            bool.must(range);
        }
        return bool;
    }

    private BoolQueryBuilder range(JsonNode data) {
        //{"field":"sdfasd","from":"","to":""
        BoolQueryBuilder bool = boolQuery();
        data.forEach((rangedata) -> {
            String field = rangedata.get("field").asText();
            RangeQueryBuilder range = rangeQuery(field);

            if (rangedata.has("from")) {
                Object from = rangedata.get("from").asText();
                range.from(from);
            }
            if (rangedata.has("to")) {
                Object to = rangedata.get("to").asText();
                range.to(to);
            }

            bool.must(range);
        });

        return bool;
    }

    private QueryBuilder termQry(String key, JsonNode value) {
        if (value.isNull()) {
            BoolQueryBuilder querybuilder = boolQuery();
            ExistsQueryBuilder exists = existsQuery(key);
            return querybuilder.mustNot(exists);
        } else {
            if (value.isArray()) {
                ArrayList<Object> list = new ArrayList<Object>();//Creating arraylist  
                value.forEach(v -> {
                    list.add(v.canConvertToInt() ? v.asInt() : v.asText());
                });
                return termsQuery(key, list);
            }
            return termQuery(key, value.asText());
        }
    }

    QueryBuilder get() throws EsQueryException {
        this.setData();
        return this.filterQuery;
    }
}
