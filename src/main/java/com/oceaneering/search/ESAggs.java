/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oceaneering.exceptions.InputException;
import com.oceaneering.properties.WrapperProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filters;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.*;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.sort.SortOrder;

/**
 *
 * @author SKashyap
 *
 */
public class ESAggs {

    private JsonNode aggsData;
    private List<AggregationBuilder> aggsQuery = new ArrayList();
    private WrapperProperties properties;
    private String separator;

    ESAggs(JsonNode data, WrapperProperties properties) {
        this.aggsData = data;
        this.properties = properties;
    }

    List<AggregationBuilder> get() throws IOException, InputException {
        this.setData();
        return this.aggsQuery;
    }

    private ESAggs setData() throws IOException, InputException {
        Iterator<JsonNode> fields = this.aggsData.iterator();

        while (fields.hasNext()) {
            JsonNode entry = fields.next();
            // iterate on sub nodes
            AggregationBuilder agg = this.createAggQuery(entry);
            this.aggsQuery.add(agg);
        }
        return this;
    }

    private AggregationBuilder createAggQuery(JsonNode entry) throws IOException, InputException {
        Iterator<Map.Entry<String, JsonNode>> sub_aggsdata = entry.fields();
        String type = null, name = null;
        JsonNode fields = null;
        int size = 0;
        JsonNode filtersData = null;
        boolean tophits = false;
        while (sub_aggsdata.hasNext()) {
            Map.Entry<String, JsonNode> entry1 = (Map.Entry<String, JsonNode>) sub_aggsdata.next();
            String key = entry1.getKey();
            if (key.equals("agg_type")) {
                type = entry1.getValue().asText();
            } else if (key.equals("agg_name")) {
                name = entry1.getValue().asText();
            } else if (key.equals("agg_field")) {
                fields = entry1.getValue();
            } else if (key.equals("agg_size")) {
                size = entry1.getValue().asInt();
            } else if (key.equals("fetchrecords")) {
                tophits = entry1.getValue().asBoolean();
            } else if (key.equals("agg_filter")) {
                filtersData = entry1.getValue();
            } else if (key.equals("agg_separator")) {
                setSeparator(entry1.getValue().asText());
            }
        }
        AggregationBuilder aggquerybuilder;
        switch (type) {
            case "count":
                aggquerybuilder = this.makeCountAggregation(name, fields, filtersData, tophits, size);
                break;
            default:
                throw new InputException(113);

        }
        return aggquerybuilder;
    }

    private AggregationBuilder makeCountAggregation(String name, JsonNode fields, JsonNode filtersData, boolean tophits, int size) {
        AggregationBuilder aggquerybuilder;
        TermsAggregationBuilder aggquery = terms(name);
        if (getSeparator() != null) {
            separator = getSeparator();
        } else {
            separator = this.properties.getAggsSeparator();
        }
        if (tophits != true) {
            IncludeExclude incExc = new IncludeExclude(null, new RegExp(".*null.*|.{0}"));
            aggquery.size(size > 0 ? size : properties.getAggCount());
            aggquery.includeExclude(incExc);
        }
        if (fields.isArray()) {
            String composite_keys = this.getCompositeKey(fields, separator);
            Map<String, String> options = new HashMap();
            options.put("source", composite_keys);
            Script script = new Script(composite_keys);
            aggquery.script(script);
        } else {
            aggquery.field(fields.asText());
        }

        if (tophits == true) {
            TopHitsAggregationBuilder tophitsquery = topHits(name + "_top_hits");
            tophitsquery.sort("_score", SortOrder.DESC);
            tophitsquery.size(this.properties.getAggSize());
            tophitsquery.fetchSource("*", "");
            aggquery.subAggregation(tophitsquery);
        }

        if (filtersData != null) {
            Iterator<Map.Entry<String, JsonNode>> filteredAggs = filtersData.fields();

            KeyedFilter keyedFilter[] = new KeyedFilter[filtersData.size()];
            int i = 0;
            while (filteredAggs.hasNext()) {
                Map.Entry<String, JsonNode> entry1 = (Map.Entry<String, JsonNode>) filteredAggs.next();
                String key_ = entry1.getKey();
                JsonNode val = entry1.getValue();
                KeyedFilter e = new KeyedFilter(key_, this.makeNestedQuery(val));
                keyedFilter[i] = e;
                i++;
            }
            FiltersAggregationBuilder filter = filters("filters_count", keyedFilter);
            aggquery.subAggregation(filter);
        }
        aggquerybuilder = aggquery;
        return aggquerybuilder;
    }

    /**
     * {"operator":"or","filter_data":[
     * {"assetworkorderdetails.startdate":{"is_nested":true,"range":{"gte":"2018-12-28"}}},
     * {"operator":"and","filter_data":[{"assetworkorderdetails.startdate":{"is_nested":true,"term":null}},{"assetworkorderdetails.endtdate":{"is_nested":true,"term":null}},{"assetworkorderdetails.startdate":{"is_nested":true,"term":null}},{"assetscheckinandoutdetails.enddate":{"is_nested":true,"term":null}}]},{"assetworkorderdetails.startdate":{"is_nested":true,"term":null}},{"assetworkorderdetails.endtdate":{"is_nested":true,"term":null}},{"assetworkorderdetails.startdate":{"is_nested":true,"term":null}},{"assetscheckinandoutdetails.enddate":{"is_nested":true,"term":null}},{"assetscheckinandoutdetails.enddate":{"is_nested":true,"range":{"lte":"2018-06-28"}}},{"assetscheckinandoutdetails.startdate":{"is_nested":true,"range":{"gte":"2018-12-28"}}},{"assetscheckinandoutdetails.enddate":{"is_nested":true,"range":{"lte":"2018-06-28"}}}]}
     *
     *
     *
     *
     *
     *
     * {"assetworkorderdetails.startdate":{"is_nested":true,"range":{"gte":"2018-12-28"}}}
     * {"operator":"and","filter_data":[{"assetworkorderdetails.startdate":{"is_nested":true,"term":null}},{"assetworkorderdetails.endtdate":{"is_nested":true,"term":null}},{"assetworkorderdetails.startdate":{"is_nested":true,"term":null}},{"assetscheckinandoutdetails.enddate":{"is_nested":true,"term":null}}]}
     * {"assetscheckinandoutdetails.enddate":{"is_nested":true,"range":{"lte":"2018-06-28"}}}
     * {"assetscheckinandoutdetails.startdate":{"is_nested":true,"range":{"gte":"2018-12-28"}}}
     * {"assetscheckinandoutdetails.enddate":{"is_nested":true,"range":{"lte":"2018-06-28"}}}
     *
     *
     * @param val
     * @param query
     * @return
     */
    private QueryBuilder makeNestedQuery(JsonNode val) {
        BoolQueryBuilder query = boolQuery();
        String operator = "and";
        String path = "";
        Boolean is_nested = false;
        if (val.has("operator")) {
            operator = val.get("operator").asText();
        }
        if (val.has("path")) {
            path = val.get("path").asText();
        }

        if (val.has("is_nested")) {
            is_nested = val.get("is_nested").asBoolean();
        }

        JsonNode queryData = val.get("filter_data");
        for (int j = 0; j < queryData.size(); j++) {
            JsonNode obj = queryData.get(j);
            QueryBuilder qry;
            if (obj.has("operator")) {
                qry = this.makeNestedQuery(obj);
            } else {
                qry = this.makeQuery(obj);
            }

            if (operator.equalsIgnoreCase("and")) {
                query.must(qry);
            } else if (operator.equalsIgnoreCase("or")) {
                query.should(qry);
            }
        }
        System.out.println(query);
        if (is_nested) {
            return nestedQuery(path, query, ScoreMode.None);
        }
        return query;
    }

    private QueryBuilder makeQuery(JsonNode obj) {
        QueryBuilder query = null;
        Iterator<Map.Entry<String, JsonNode>> objItr = obj.fields();
        while (objItr.hasNext()) {
            Map.Entry<String, JsonNode> dataset = (Map.Entry<String, JsonNode>) objItr.next();
            String fieldName = dataset.getKey();
//
            JsonNode queryData = dataset.getValue();
            boolean is_nested = false;
            String[] path = null;
            if (queryData.has("is_nested") && queryData.get("is_nested").asBoolean() == true) {
                is_nested = true;
                path = fieldName.split("\\.");
            }
            if (queryData.has("term")) {
                if (queryData.get("term").asText() == null) {
                    query = this.makeNullTermQuery(fieldName, is_nested, path[0]);
                    is_nested = false;
                } else {
                    query = this.makeTermQuery(fieldName, queryData.get("term"));
                }
            } else if (queryData.has("range")) {
                query = this.makeRangeQuery(fieldName, queryData.get("range"));
            }

            if (is_nested) {
                query = nestedQuery(path[0], query, ScoreMode.None);
            }
        }

        return query;
    }

    private QueryBuilder makeTermQuery(String fieldName, JsonNode qryData) {
        QueryBuilder term;
        if (qryData.asText() == "null") {
            BoolQueryBuilder bool = boolQuery();
            ExistsQueryBuilder exists = existsQuery(fieldName);

            bool.mustNot(exists);
            term = bool;
        } else {
            term = termQuery(fieldName, qryData.asText());
        }
        return term;
    }

    private QueryBuilder makeNullTermQuery(String fieldName, boolean is_nested, String path) {
        BoolQueryBuilder bool = boolQuery();
        QueryBuilder exists = existsQuery(fieldName);
        if (is_nested) {
            exists = nestedQuery(path, exists, ScoreMode.None);
        }
        bool.mustNot(exists);
        return bool;
    }

    private RangeQueryBuilder makeRangeQuery(String fieldName, JsonNode qryData) {
        RangeQueryBuilder range = rangeQuery(fieldName);
        if (qryData.has("gte")) {
            range.gte(qryData.get("gte").textValue());
        } else if (qryData.has("gt")) {
            range.gt(qryData.get("gt").textValue());
        }

        if (qryData.has("lte")) {
            range.lte(qryData.get("lte").textValue());
        } else if (qryData.has("lt")) {
            range.lt(qryData.get("lt").textValue());
        }

        return range;
    }

    private String getCompositeKey(JsonNode fields, String delimiter) {
        String compositeKeys = "";
        if (delimiter == null) {
            delimiter = "-";
        }
        for (int i = 0; i < fields.size(); i++) {
            compositeKeys += "doc['" + fields.get(i).asText() + "'].value";
//            compositeKeys += ((i != fields.size() - 1) ? "+'-'+" : "");
            compositeKeys += ((i != fields.size() - 1) ? "+'" + delimiter + "'+" : "");
        }
        return compositeKeys;
    }

    /**
     * @return the separator
     */
    public String getSeparator() {
        return separator;
    }

    /**
     * @param separator the separator to set
     */
    public void setSeparator(String separator) {
        this.separator = separator;
    }
}
