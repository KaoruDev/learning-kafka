package com.kaoruk;

import org.apache.http.Consts;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kaoru on 1/6/17.
 */
public class MyHttpClient {
    public static class Builder {
        private Map<String, String> headers = new HashMap<>();
        private List<NameValuePair> params = new ArrayList<>();
        private String url;

        public String getUrl() {
            return url;
        }

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder setHeaders(String key, String value) {
            headers.put(key, value);
            return this;
        }

        public Builder setParam(String key, String value) {
            params.add(new BasicNameValuePair(key, value));
            return this;
        }
    }

    public static class MyResponse {
        private StatusLine statusLine;
        private JSONObject body;
        private int statusCode;

        public MyResponse(int code) {
            this.statusCode = code;
        }

        public void setBody(JSONObject body) {
            this.body = body;
        }

        public boolean ok() {
            return !errored();
        }

        public boolean errored() {
           return statusCode > 299;
        }

        public JSONObject getBody() {
            return body;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public StatusLine getStatusLine() {
            return statusLine;
        }

        public void setStatusLine(StatusLine statusLine) {
            this.statusLine = statusLine;
        }
    }

    public static MyResponse post(Builder builder) {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost request = new HttpPost(builder.getUrl());

        for (Map.Entry<String, String> entry: builder.headers.entrySet()) {
            request.setHeader(entry.getKey(), entry.getValue());
        }

        if (!builder.params.isEmpty()) {
            request.setEntity(new UrlEncodedFormEntity(builder.params, Consts.UTF_8));
        }

        try (CloseableHttpResponse response = client.execute(request)) {

            MyResponse responseObj = new MyResponse(response.getStatusLine().getStatusCode());

            if (responseObj.ok()) {
                responseObj.setBody(new JSONObject(EntityUtils.toString(response.getEntity())));
            } else {
                responseObj.setStatusLine(response.getStatusLine());
            }

            EntityUtils.consume(response.getEntity());

            return responseObj;
        } catch (IOException e) {
            System.out.println("Http Request closed unexpectedly.");
            e.printStackTrace();
            return null;
        }
    }
}
