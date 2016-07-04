package io.hops.kafkautil;

import java.util.Properties;
import java.util.logging.Logger;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;

/**
 * Hops utility class to be used by applications that want to communicate with
 * Kafka.
 * <p>
 */
public class HopsKafkaUtil {

    private static final Logger logger = Logger.getLogger(HopsKafkaUtil.class.
            getName());

    private static HopsKafkaUtil instance = null;

    private String jSessionId;
    private Integer projectId;
    private String topicName;
    private String brokerEndpoint;
    private String restEndpoint;
    private String domain;
    private String keyStore;
    private String trustStore;

    private HopsKafkaUtil() {

    }

    /**
     * Setup the Kafka instance.
     */
    public void setup() {
        Properties sysProps = System.getProperties();

        //validate arguments first
        this.jSessionId = sysProps.getProperty("kafka.sessionid");
        this.projectId = Integer.parseInt(sysProps.getProperty("kafka.projectid"));
        this.brokerEndpoint = sysProps.getProperty("kafka.brokeraddress");//"10.0.2.15:9091";
        this.restEndpoint = "https://hops.site:443/hopsworks/api/project";
        this.domain = "hops.site";
        this.keyStore = "kafka_k_certificate";//sysProps.getProperty("kafka_k_certificate");
        this.trustStore = "kafka_t_certificate";//"sysProps.getProperty("kafka_t_certificate");
    }

    /**
     * Setup the Kafka instance. Endpoint is where the REST API listens for
     * requests. I.e. http://localhost:8080/. Similarly set domain to
     * "localhost"
     * <p>
     * @param endpoint
     * @param domain
     */
    public void setup(String endpoint, String domain) {
        Properties sysProps = System.getProperties();

        //validate arguments first
        this.jSessionId = sysProps.getProperty("kafka.sessionid");
        this.projectId = Integer.parseInt(sysProps.getProperty("kafka.projectid"));
        this.brokerEndpoint = sysProps.getProperty("kafka.brokeraddress");
        this.restEndpoint = endpoint + "/hopsworks/api/project";
        this.domain = domain;
        this.keyStore = "kafka_k_certificate";
        this.trustStore = "kafka_t_certificate";
    }

    /**
     * Setup the Kafka instance. Endpoint is where the REST API listens for
     * requests. I.e. http://localhost:8080/. Similarly set domain to
     * "localhost" KeyStore and TrustStore locations should on the local
     * machine.
     *
     * @param topicName
     * @param endpoint
     * @param keyStore
     * @param trustStore
     * @param domain
     */
    public void setup(String topicName, String endpoint, String keyStore,
            String trustStore, String domain) {
        Properties sysProps = System.getProperties();

        //validate arguments first
        this.jSessionId = sysProps.getProperty("kafka.sessionid");
        this.projectId = Integer.parseInt(sysProps.getProperty("kafka.projectid"));
        this.topicName = topicName;
        this.brokerEndpoint = sysProps.getProperty("kafka.brokeraddress");
        this.restEndpoint = endpoint + "/hopsworks/api/project";
        this.domain = domain;
        this.keyStore = keyStore;
        this.trustStore = trustStore;

    }

    public void setup(String jSessionId, int projectId, String topicName, String domain, String brokerEndpoint, String restEndpoint,
            String keyStore, String trustStore) {
        this.jSessionId = jSessionId;
        this.projectId = projectId;
        this.topicName = topicName;
        this.domain = domain;
        this.brokerEndpoint = brokerEndpoint;
        this.restEndpoint = restEndpoint + "/hopsworks/api/project";
        this.keyStore = keyStore;
        this.trustStore = trustStore;
    }

    public static HopsKafkaUtil getInstance() {
        if (instance == null) {
            instance = new HopsKafkaUtil();
        }
        return instance;
    }

    public HopsKafkaConsumer getHopsKafkaConsumer(String topic) throws
            SchemaNotFoundException {
        return new HopsKafkaConsumer(topic);
    }

    public HopsKafkaProducer getHopsKafkaProducer(String topic) throws
            SchemaNotFoundException {
        return new HopsKafkaProducer(topic);
    }

    public HopsKafkaProducer getHopsKafkaProducer(String topic, long lingerDelay) throws SchemaNotFoundException {
        return new HopsKafkaProducer(topic, lingerDelay);
    }

    /**
     * @Deprecated. @return
     */
    protected Properties getProducerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerEndpoint);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        //configure the ssl parameters
        if (!(keyStore.isEmpty() && keyStore == null)) {
            props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore);
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "adminpw");
            props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStore);
            props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "adminpw");
        }
        return props;
    }

    /**
     * @Deprecated. @return
     */
    protected Properties getConsumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerEndpoint);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        //configure the ssl parameters
        if (trustStore != null && !trustStore.isEmpty()
                && keyStore != null && !keyStore.isEmpty()) {
            props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore);
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "adminpw");
            props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStore);
            props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "adminpw");
        }

        return props;
    }

    public String getDomain() {
        return domain;
    }

    public String getBrokerEndpoint() {
        return brokerEndpoint;
    }

    public static Logger getLogger() {
        return logger;
    }

    public String getjSessionId() {
        return jSessionId;
    }

    public Integer getProjectId() {
        return projectId;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getRestEndpoint() {
        return restEndpoint;
    }

    public String getKeyStore() {
        return keyStore;
    }

    public String getTrustStore() {
        return trustStore;
    }

}
