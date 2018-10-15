/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oceaneering.properties.NetworkProps;
import com.oceaneering.properties.WrapperProperties;
import java.io.IOException;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClientBuilder;

/**
 *
 * @author SKashyap
 *
 */
public class EsRequest {

    private String query;
    private String id;
    private WrapperProperties properties;
    private String method = "GET";
    private String endpoint = "";
    private Map<String, String> headers;
    private Map<String, String> urlParams;
    private Map<String, String> params;
    private boolean debug = false;
    private String index = null;
    private String type = null;
    private String error = null;
    private String status = null;
    private Response response = null;

    public EsRequest(String query, WrapperProperties properties) throws IOException {
        this.query = query;
        this.properties = properties;
    }

    public EsRequest(String query) throws IOException {
        this.query = query;
    }

    public EsRequest() throws IOException {
    }

    public JsonNode post() throws IOException {
        this.method = "POST";
        return this.send().getResponse();
    }

    public JsonNode get() throws IOException {
        this.method = "GET";
        return this.send().getResponse();
    }

    public String put() throws IOException {
        this.method = "PUT";
        return this.send().getStatus();
    }

    public String head() throws IOException {
        this.method = "HEAD";
        return this.send().getStatus();
    }

    public JsonNode delete() throws IOException {
        this.method = "DELETE";
        return this.send().getResponse();
    }

    public EsRequest setHeader(Map urlParams) {
        this.urlParams = urlParams;
        return this;
    }

    public EsRequest setIndex(String index) {
        this.index = index;
        return this;
    }

    public EsRequest setType(String type) {
        this.type = type;
        return this;
    }

    public EsRequest debug() {
        this.debug = true;
        return this;
    }

    public EsRequest setEndPoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public EsRequest setUrlQueryParams(Map params) {
        this.params = params;
        return this;
    }

    public EsRequest setId(String id) {
        this.id = id;
        return this;
    }

    private JsonNode getResponse() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        if (this.error != null) {
            return mapper.readValue(error, JsonNode.class);
        }
        HttpEntity entity = this.response.getEntity();
        if (entity != null) {
            return mapper.readValue(entity.getContent(), JsonNode.class);
        }
        return mapper.readValue("{}", JsonNode.class);
    }

    private String getStatus() {
        return this.status;
    }

    public EsRequest send() {

        String index = this.index;
        if (this.index == null) {
            if (properties instanceof WrapperProperties) {
                index = properties.getIndex().asText();
            }
        }

        NetworkProps net = NetworkProps.Instance();
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(net.user, net.pass));
        RestClient restClient = RestClient.builder((HttpHost[]) new HttpHost[]{new HttpHost(net.host, net.port, net.scheme)})
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                }).build();

        try {
            if (this.params == null) {
                Map<String, String> str = new HashMap();
                this.params = str;
            }
            String url = "/";
            if (index != null) {
                url += index;

                if (this.type != null) {
                    url += "/" + this.type;
                }
            }

            if (this.endpoint != "") {
                url += "/" + this.endpoint;
            }
            if (this.query != null) {
                NStringEntity entity = new NStringEntity(this.query, ContentType.APPLICATION_JSON);
                this.response = restClient.performRequest(this.method, url, this.params, (HttpEntity) entity, new Header[0]);

            } else {
                this.response = restClient.performRequest(this.method, url, this.params, new Header[0]);
                this.status = this.response.getStatusLine().toString();

            }
            restClient.close();
            return this;
        } catch (ResponseException ex) {
            if (ex.getLocalizedMessage().contains("Fielddata is disabled on text fields by default")) {
                this.error = "{ \"error\": \"Fielddata is disabled on text fields by default\"}";
            }
            if (ex.getLocalizedMessage().contains("404 Not Found")) {
                this.error = "{ \"error\": \"404 - Not Found\"}";
            }
            //exception handle added when there is no index available, 500 error crashing the flow
            if (ex.getLocalizedMessage().contains("failed to parse date field")) {
                this.error = "{ \"error\": \"No content to map due to end-of-input\"}";
            }
        } catch (ConnectException cn) {
            this.error = "{ \"error\": \"Error communicating to elasticSearch server.\"}";
        } catch (Exception ex) {
            this.error = "{ \"error\": \"There is some technical problem. Please contact administrator\"}";
        }
        return this;
    }
}
