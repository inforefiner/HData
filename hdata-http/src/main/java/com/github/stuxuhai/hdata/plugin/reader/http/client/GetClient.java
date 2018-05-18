package com.github.stuxuhai.hdata.plugin.reader.http.client;


import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class GetClient extends BaseClient {

    public GetClient(String baseUrl) {
        super(baseUrl);
    }

    public GetClient doGet() {
        Response response = getResource().request(MediaType.APPLICATION_JSON_TYPE)
                .accept(MediaType.APPLICATION_JSON)
                .get();
        setResponse(response);
        return this;
    }
}