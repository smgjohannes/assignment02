import ballerina/io;
import ballerina/uuid;
import ballerinax/kafka;
import ballerina/http;

kafka:ProducerConfiguration producerConfigs = {
    clientId: "storage-system-producer",
    acks: "all",
    retryCount: 3,
    enableIdempotence: true,
    sendBuffer: 30
};

final string DEFAULT_URL = "localhost:9092";

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, producerConfigs);

service /storageSystem on new http:Listener(9090) {

    @http:ResourceConfig {consumes: ["application/json"]}
    resource function post create(http:Caller caller, http:Request request) {
        http:Response response = new ();
        json|error reqPayload = request.getJsonPayload();

        if (reqPayload is json) {
            json|error message = reqPayload.data;

            if (message is json) {
                json storageInfo = {"data": message};
                byte[] serializedMessage = storageInfo.toBalString().toBytes();

                var responseResult = SendKafkaTopic(serializedMessage);

                if (responseResult is kafka:Error) {
                    response.statusCode = 500;
                    string errorMessage = "Error occurred when sending message " + responseResult.toBalString();
                    response.setJsonPayload({"Message": errorMessage});
                    var result = caller->respond(response);
                    io:println("Kafka producer failed to send data", result);
                } else {
                    response.statusCode = 201;
                    response.setJsonPayload({"Status": "Success"});
                    var result = caller->respond(response);
                    io:println("Kafka producer successfully send data", result);
                }
            }
        }

    }

}

function SendKafkaTopic(byte[] message) returns error? {

    check kafkaProducer->send({
            topic: "storage-system",
            value: message,
            'key: uuid:createType1AsString().toBytes()
        });

    check kafkaProducer->'flush();
}
