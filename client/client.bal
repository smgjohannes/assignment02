import ballerinax/kafka;
import ballerina/log;

kafka:ConsumerConfiguration consumerConfigs = {
    concurrentConsumers: 2,
    offsetReset: "earliest",
    groupId: "group-id",
    topics: ["storage-system"],
    pollingInterval: 1
};

final string DEFAULT_URL = "localhost:9092, localhost:9093, localhost:9094";

listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, consumerConfigs);

//Kafka service that listens from the topic 'storage-system'
service kafka:Service on kafkaListener {
    remote function onConsumerRecord(
        kafka:Caller caller,kafka:ConsumerRecord[] records) returns error? {

        foreach var kafkaRecord in records {
            check processKafkaRecord(kafkaRecord);
        }

    }
}

function processKafkaRecord(kafka:ConsumerRecord kafkaRecord) returns error? {
    //The value must  be a byte[]  because the byte[] deserializer is used for the value.
    byte[] messageContent = kafkaRecord.value;
    //Converting byte  to a string
    string message = check string:fromBytes(messageContent);
    log:printInfo("Received Message: " + message);
}
