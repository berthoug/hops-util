/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.util.dela;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import io.hops.util.HopsUtil;
import io.hops.util.SchemaNotFoundException;
import java.util.logging.Logger;
import javax.ws.rs.core.Cookie;
import org.apache.avro.Schema;
import org.json.JSONObject;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsHelper {

    private static final Logger logger = Logger.getLogger(HopsHelper.class.getName());

    public static HopsConsumer getHopsConsumer(String topic)
            throws SchemaNotFoundException {
        HopsUtil config = HopsUtil.getInstance();
        String stringSchema = getSchemaByTopic(config, topic);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(stringSchema);
        return new HopsConsumer(topic, schema);
    }

    public static HopsProducer getHopsProducer(String topic, long lingerDelay)
            throws SchemaNotFoundException {
        HopsUtil config = HopsUtil.getInstance();
        String stringSchema = getSchemaByTopic(config, topic);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(stringSchema);
        return new HopsProducer(topic, schema, lingerDelay);
    }

    public static String getSchemaByTopic(HopsUtil config, String topicName)
            throws SchemaNotFoundException {
        return getSchemaByTopic(config.getRestEndpoint(), config.getjSessionId(), config.getProjectId(), topicName);
    }

    public static String getSchemaByTopic(String restEndpoint, String jSessionId, int projectId, String topicName) throws SchemaNotFoundException {
        return getSchemaByTopic(restEndpoint, jSessionId, projectId, topicName, Integer.MIN_VALUE);
    }

    public static String getSchemaByTopic(String restEndpoint, String jSessionId, int projectId, String topicName, int versionId) throws SchemaNotFoundException {

        String uri = restEndpoint + "/" + projectId + "/kafka/schema/" + topicName;
        if (versionId > 0) {
            uri += "/" + versionId;
        }
        logger.info("getting schema:" + uri);
        ClientResponse response = getResponse(uri, jSessionId);
        if (response == null) {
            throw new SchemaNotFoundException("Could not reach schema endpoint");
        } else if (response.getStatus() != 200) {
            throw new SchemaNotFoundException(response.getStatus(), "Schema is not found");
        }
        String schema = extractSchema(response);
        return schema;

    }

    public static String getSchemaByName(String restEndpoint, String jSessionId, int projectId, String schemaName, int schemaVersion) throws SchemaNotFoundException {
        String uri = restEndpoint + "/" + projectId + "/kafka/showSchema/" + schemaName + "/" + schemaVersion;
        logger.info("getting schema:" + uri);
        ClientResponse response = getResponse(uri, jSessionId);
        if (response == null) {
            throw new SchemaNotFoundException("Could not reach schema endpoint");
        } else if (response.getStatus() != 200) {
            throw new SchemaNotFoundException(response.getStatus(), "Schema is not found");
        }
        //logger.log(Level.INFO, "Response:{0}", response.toString());
        String schema = extractSchema(response);
        return schema;
    }

    private static ClientResponse getResponse(String uri, String jSessionId) {
        ClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        WebResource service = client.resource(uri);
        Cookie cookie = new Cookie("SESSIONID", jSessionId);
        return service.cookie(cookie).get(ClientResponse.class);
    }

    private static String extractSchema(ClientResponse response) {
        String content = response.getEntity(String.class);
        //Extract fields from json
        JSONObject json = new JSONObject(content);
        String schema = json.getString("contents");
        schema = tempHack(schema);
        return schema;
    }

    private static String tempHack(String schema) {
        int actualSchema = schema.indexOf('{');
        return schema.substring(actualSchema);
    }
    
}
