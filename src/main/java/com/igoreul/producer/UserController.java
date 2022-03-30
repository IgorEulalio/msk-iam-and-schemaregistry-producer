package com.igoreul.producer;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.igoreul.User;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import software.amazon.awssdk.services.glue.model.DataFormat;

import javax.validation.Valid;
import java.util.Properties;
import java.util.UUID;

@RestController
@RequestMapping("/users")
public class UserController {

    private final static Logger logger = LoggerFactory.getLogger(UserController.class.getName());
    @Value("${bootstrap.servers}")
    private String bootstrapServers;
    @Value("${region.name}")
    private String regionName;
    @Value("${topic}")
    private String topic;
    @Value("${truststore-key}")
    private String truststoreKey;

    @PostMapping(value = "/")
    public ResponseEntity<UserDTO> post(@RequestBody @Valid UserDTO userDTO){

        User user = mapToEntity(userDTO);
        sendUserToKafka(user);
        return ResponseEntity.ok(userDTO);
    }

    private void sendUserToKafka(User user) {
        KafkaProducer<String, User> producer = getKafkaProducer();
        logger.info("Starting to send records...");
        ProducerRecord<String, User> record = new ProducerRecord<String, User>(topic, user.getId(), user);
        producer.send(record, new ProducerCallback());
    }

    private KafkaProducer<String, User> getKafkaProducer() {
        return new KafkaProducer<String,User>(getProducerConfig());
    }

    private Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"msk-cross-account-gsr-producer");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        props.put("security.protocol", "SASL_SSL");
        props.put("ssl.truststore.location", truststoreKey);
        props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        props.put(AWSSchemaRegistryConstants.AWS_REGION, regionName);
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "my-registry");
        props.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "my-schema");
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());
//        Without this field, you need to create and register your schema in advance
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);

        return props;
    }

    private User mapToEntity(UserDTO dto) {
        UUID uuid = UUID.randomUUID();
        return new User(uuid.toString(), dto.getName(), dto.getFavoriteNumber(), dto.getFavoriteColor());
    }

    private class ProducerCallback implements Callback {
        /**
         * This method is a callback method which gets
         * invoked when Kafka producer receives the messages delivery
         * acknowledgement.
         * @return Nothing
         */
        @Override
        public void onCompletion(RecordMetadata recordMetaData, Exception e){
            if (e == null) {
                logger.info("Received new metadata. \n" +
                        "Topic:" + recordMetaData.topic() + "\n" +
                        "Partition: " + recordMetaData.partition() + "\n" +
                        "Offset: " + recordMetaData.offset() + "\n" +
                        "Timestamp: " + recordMetaData.timestamp());
            }
            else {
                logger.info("There's been an error from the Producer side");
                e.printStackTrace();
            }
        }
    }
}
