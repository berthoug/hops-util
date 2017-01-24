package io.hops.util.dela;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import io.hops.util.HopsProcess;
import io.hops.util.HopsProcessType;
import io.hops.util.HopsUtil;
import io.hops.util.SchemaNotFoundException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * Utility class that sends messages to the Kafka service.
 */
public class HopsProducer extends HopsProcess {

  private static final Logger logger = Logger.
          getLogger(HopsProducer.class.getName());

  private final KafkaProducer<String, byte[]> producer;
  private final Injection<GenericRecord, byte[]> recordInjection;

  /**
   * Create a Producer to stream messages to Kafka.
   *
   * @param topic
   * @throws SchemaNotFoundException
   */
  HopsProducer(String topic, long lingerDelay) throws SchemaNotFoundException {
    super(HopsProcessType.PRODUCER, topic);
    Properties props = HopsUtil.getKafkaProperties().
            defaultProps();
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "HopsProducer");
    props.put(ProducerConfig.LINGER_MS_CONFIG, lingerDelay); 
    producer = new KafkaProducer<>(props);
    recordInjection = GenericAvroCodecs.toBinary(schema);
  }
  
  public HopsProducer(String topic) throws SchemaNotFoundException {
    this(topic, 0);
  }
  
  /**
   * No exception in constructors - using Helper static methods to get schema to get rid of exceptions
   * @param topic
   * @param schema
   * @param lingerDelay 
   */
  HopsProducer(String topic, Schema schema, long lingerDelay) {
      super(HopsProcessType.PRODUCER, topic, schema);
      Properties props = HopsUtil.getKafkaProperties().
            defaultProps();
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "HopsProducer");
    props.put(ProducerConfig.LINGER_MS_CONFIG, lingerDelay); 
    producer = new KafkaProducer<>(props);
    recordInjection = GenericAvroCodecs.toBinary(schema);
  }

  /**
   * Send the given record to Kafka.
   * <p>
   * @param messageFields
   */
    public void produce(Map<String, Object> messageFields) {
        //create the avro message
        GenericData.Record avroRecord = new GenericData.Record(schema);
        for (Map.Entry<String, Object> message : messageFields.entrySet()) {
            //TODO: Check that messageFields are in avro record
            avroRecord.put(message.getKey(), message.getValue());
        }
        produce(avroRecord);
    }

    public void produce(GenericRecord avroRecord) {
        byte[] bytes = recordInjection.apply(avroRecord);
        produce(bytes);
    }

    public void produce(byte[] byteRecord) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, byteRecord);
        producer.send(record);
    }
    
    public byte[] prepareRecord(GenericRecord avroRecord) {
        byte[] bytes = recordInjection.apply(avroRecord);
        return bytes;
    }

  @Override
  public void close() {
    producer.close();
  }

}

/*
 * class DemoCallBack implements Callback {
 *
 * private static final Logger logger = Logger.
 * getLogger(DemoCallBack.class.getName());
 *
 * private final long startTime;
 * private final int key;
 * private final String message;
 *
 * public DemoCallBack(long startTime, int key, String message) {
 * this.startTime = startTime;
 * this.key = key;
 * this.message = message;
 * }
 */
/**
 * A callback method the user can implement to provide asynchronous handling
 * of request completion. This method will be called when the record sent to
 * the server has been acknowledged. Exactly one of the arguments will be
 * non-null.
 *
 * @param metadata The metadata for the record that was sent (i.e. the
 * partition and offset). Null if an error occurred.
 * @param exception The exception thrown during processing of this record.
 * Null if no error occurred.
 */
/*
 * public void onCompletion(RecordMetadata metadata, Exception exception) {
 *
 * if (metadata != null) {
 * logger.log(Level.SEVERE, "Message {0} is sent",
 * new Object[]{message});
 * } else {
 * exception.printStackTrace();
 * }
 * }
 * }
 */
