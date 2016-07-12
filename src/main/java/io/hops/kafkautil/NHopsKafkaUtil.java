/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.kafkautil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.json.JSONObject;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NHopsKafkaUtil {

    private static final Logger logger = Logger.getLogger(HopsKafkaUtil.class.getName());

    public static String getSchemaByTopic(HopsKafkaUtil config, String topicName) throws SchemaNotFoundException {
        return getSchemaByTopic(config.getDomain(), config.getRestEndpoint(), config.getjSessionId(), config.getProjectId(), topicName);
    }
    
    public static String getSchemaByTopic(String domain, String restEndpoint, String jSessionId, int projectId, String topicName) throws SchemaNotFoundException {
        return getSchemaByTopic(domain, restEndpoint, jSessionId, projectId, topicName, Integer.MIN_VALUE);
    }

    public static String getSchemaByTopic(String domain, String restEndpoint, String jSessionId, int projectId, String topicName, int versionId) throws SchemaNotFoundException {

        String uri = restEndpoint + "/" + projectId + "/kafka/schema/" + topicName;
        if (versionId > 0) {
            uri += "/" + versionId;
        }

        HttpResponse response = httpGet(jSessionId, domain, uri);
        if (response == null) {
            throw new SchemaNotFoundException("Could not reach schema endpoint");
        } else if (response.getStatusLine().getStatusCode() != 200) {
            throw new SchemaNotFoundException(response.getStatusLine().getStatusCode(),
                    "Schema is not found");
        }
        String schema = extractSchema(response);
        return schema;

    }

    public static String getSchemaByName(String domain, String restEndpoint, String jSessionId, int projectId, String schemaName, int schemaVersion) throws SchemaNotFoundException {
        String uri = restEndpoint + "/" + projectId + "/kafka/showSchema/" + schemaName + "/" + schemaVersion;
        HttpResponse response = httpGet(jSessionId, domain, uri);
        if (response == null) {
            throw new SchemaNotFoundException("Could not reach schema endpoint");
        } else if (response.getStatusLine().getStatusCode() != 200) {
            throw new SchemaNotFoundException(response.getStatusLine().getStatusCode(), "Schema is not found");
        }
        //logger.log(Level.INFO, "Response:{0}", response.toString());
        String schema = extractSchema(response);
        return schema;
    }

    private static HttpResponse httpGet(String jSessionId, String domain, String uri) {
        //Setup the REST client to retrieve the schema
        BasicCookieStore cookieStore = new BasicCookieStore();
        BasicClientCookie cookie = new BasicClientCookie("SESSIONID", jSessionId);
        cookie.setDomain(domain);
        cookie.setPath("/");
        cookieStore.addCookie(cookie);
        HttpClient client = HttpClientBuilder.create().setDefaultCookieStore(cookieStore).build();

        final HttpGet request = new HttpGet(uri);

        HttpResponse response = null;
        try {
            response = client.execute(request);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, ex.getMessage());
        }
        return response;
    }

    private static String extractSchema(HttpResponse response) {
        StringBuilder result = new StringBuilder();
        try {
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
        } catch (IOException ex) {
            logger.log(Level.SEVERE, ex.getMessage());
        }

        //Extract fields from json
        JSONObject json = new JSONObject(result.toString());
        String schema = json.getString("contents");
        schema = tempHack(schema);
        return schema;
    }
    
    private static String tempHack(String schema) {
        int actualSchema = schema.indexOf('{');
        return schema.substring(actualSchema);
    }
}
