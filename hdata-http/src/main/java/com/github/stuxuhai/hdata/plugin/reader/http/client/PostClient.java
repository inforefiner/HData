package com.github.stuxuhai.hdata.plugin.reader.http.client;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;


public class PostClient extends BaseClient {

    public PostClient(String baseUrl) {
        super(baseUrl);
    }

    public PostClient doPost(Object params) {
        WebTarget resource = getResource();
        Invocation.Builder builder;
        String type = MediaType.APPLICATION_JSON;
        if (params instanceof String || params instanceof List) {
            builder = resource.request(type);
        } else {
            builder = resource.request(type = MediaType.APPLICATION_FORM_URLENCODED);
        }
        builder = builder.accept(MediaType.APPLICATION_JSON);
        try {
            Response response = builder
                    .post(Entity.entity(params, type));
            setResponse(response);
        } catch (Exception e) {
            throw e;
        }
        return this;
    }
}