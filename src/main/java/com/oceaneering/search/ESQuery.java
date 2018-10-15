/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oceaneering.common.Utility;
import com.oceaneering.exceptions.EsQueryException;
import com.oceaneering.exceptions.InputException;
import com.oceaneering.properties.NetworkProps;
import com.oceaneering.properties.WrapperProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.elasticsearch.index.query.BoolQueryBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import static org.elasticsearch.index.query.Operator.fromString;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 *
 * @author SKashyap
 *
 */
public class ESQuery {

    private JsonNode queryParams;
    private JsonNode query;
    private Integer size = null;
    private int offset = 0;
    private BoolQueryBuilder mainQuery;
    private List<AggregationBuilder> aggs = new ArrayList();
    private QueryBuilder filter;
//    private String[] source;
    private WrapperProperties properties;
    private NetworkProps netProps;
    private boolean debug = false;
    private JsonNode source;

    public ESQuery(String arrayBody, WrapperProperties properties) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        this.queryParams = mapper.readTree(arrayBody);
        this.properties = properties;
        this.setupNetworkConfig();
        this.mainQuery = boolQuery();
    }

    public ESQuery(String arrayBody, WrapperProperties properties, JsonNode source) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        this.queryParams = mapper.readTree(arrayBody);
        this.properties = properties;
        this.setupNetworkConfig();
        this.mainQuery = boolQuery();
        this.source = source;
    }

    private void setupNetworkConfig() {
        this.netProps = NetworkProps.Instance();
    }

    public String get() throws IOException, EsQueryException, InputException {
        return this.setData().build();
    }

    private ESQuery setData() throws IOException, EsQueryException, InputException {
// because filter should be prepared before the query builds
        String filter = this.queryParams.has("filter") ? this.queryParams.get("filter").toString() : "";
        if (filter != "") {
            this.filter(this.queryParams.get("filter"));
        }
        Iterator<Map.Entry<String, JsonNode>> fields = this.queryParams.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) fields.next();
            String key = entry.getKey();
            JsonNode value = entry.getValue();
            switch (key) {
                case "search_keyword":
                    this.searchKeyword(value);
                    break;
                case "search_term":
                    this.searchTerm(value);
                    break;
                case "page_size":
                    this.pageSize(value);
                    break;
                case "from":
                case "offset":
                    this.offset(value);
                    break;
                case "aggs":
                    this.aggs(value);
                    break;
                case "debug":
                    this.debug = value.asBoolean();
                    break;
            }
        }
        return this;
    }

    private String build() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(this.properties.getIndex().asText());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (this.filter != null) {
            this.mainQuery.filter(this.filter);
        }
        searchSourceBuilder.query(this.mainQuery);
        if (this.aggs != null) {
            for (int i = 0; i < this.aggs.size(); i++) {
                searchSourceBuilder.aggregation(this.aggs.get(i));
            }

        }
        SearchResponse response = null;
        searchSourceBuilder.size(this.getSize());
        searchSourceBuilder.from(this.offset);
        searchSourceBuilder.fetchSource(this.getSource(), null);

        //searchSourceBuilder.sort("_score", SortOrder.DESC);
        // searchSourceBuilder.sort("_id", SortOrder.ASC);
        return searchSourceBuilder.toString();
    }

    private String[] getSource() {
        int s = this.source.size();
        String[] source = new String[s];
        for (int i = 0; i < s; i++) {
            source[i] = this.source.get(i).asText();
        }
        return source;
    }

    private void aggs(JsonNode data) throws IOException, InputException {
        this.aggs = (new ESAggs(data, this.properties)).get();
    }

    private void filter(JsonNode data) throws EsQueryException {
        this.filter = (new ESFilter(data, this.properties)).get();
    }

    public void searchKeyword(JsonNode data) throws IOException, InputException {
        String[] keywords = Utility.split(data.asText());
        BoolQueryBuilder searchQuery = boolQuery();
        FunctionScoreQueryBuilder funcQry;
        ConstantScoreQueryBuilder constquery;
        for (int idx = 0; idx < keywords.length; idx++) {
            String keyword = "*" + keywords[idx] + "*";
            QueryStringQueryBuilder qr = queryStringQuery(keyword);
            qr.fields(this.properties.getSearchableFields());
            constquery = constantScoreQuery(qr);
            constquery.boost(new Float("1.5"));
            searchQuery.should(constquery);

            keyword = keywords[idx] + "*";
            qr = queryStringQuery(keyword);
            qr.fields(this.properties.getSearchableFields());
            constquery = constantScoreQuery(qr);
            constquery.boost(new Float("1.5"));
            searchQuery.should(constquery);
        }
        if (keywords.length == 1) {
            QueryStringQueryBuilder qr = queryStringQuery(data.asText() + "*");
            qr.fields(this.properties.getSearchableLoweredFields());
            constquery = constantScoreQuery(qr);
            constquery.boost(new Float("1.5"));
            searchQuery.should(constquery);
        }
        SimpleQueryStringBuilder sqs = simpleQueryStringQuery(data.asText().replaceAll("/", " "));
        sqs.fields(this.properties.getSearchableFields());
        constquery = constantScoreQuery(sqs);
        constquery.boost(new Float("1.5"));
        searchQuery.should(constquery);

        sqs.defaultOperator(fromString("and"));
        constquery = constantScoreQuery(sqs);
        constquery.boost(new Float("1.5"));
        searchQuery.should(constquery);

        this.mainQuery.must(searchQuery);
    }

    public void searchTerm(JsonNode data) throws IOException, InputException {
        BoolQueryBuilder searchQuery = boolQuery();
        this.properties.getSearchableLoweredFields().forEach((key, score) -> {
            searchQuery.should(termQuery(key, data.asText()));
        });
        this.mainQuery.must(searchQuery);
    }

    private void pageSize(JsonNode data) {
        this.size = data.asInt();
    }

    private int getSize() {
        if (this.size != null) {
            return this.size;
        }
        return this.properties.getSourceSize();
    }

    private void offset(JsonNode data) {
        this.offset = data.asInt();
    }
}
