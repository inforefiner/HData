package com.github.stuxuhai.hdata.plugin.reader.http.client;


import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.filter.EncodingFilter;
import org.glassfish.jersey.message.GZipEncoder;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

public class BaseClient {
    private Client client;
    private String url;
    private Response response;
    private int timeout = 60 * 1000 * 5;

    public BaseClient(String url) {
        this.client = ClientBuilder.newClient();
        client.property(ClientProperties.CONNECT_TIMEOUT, timeout);
        client.property(ClientProperties.READ_TIMEOUT, timeout);
        client.register(GZipEncoder.class);
        client.register(EncodingFilter.class);
        this.url = url;
    }

    protected WebTarget getResource() {
        return client.target(url);
    }

    protected void setResponse(Response response) {
        this.response = response;
    }

    public Object getResponse() {
        if (response != null) {
            return response.readEntity(String.class);
        }
        return null;
    }
}
