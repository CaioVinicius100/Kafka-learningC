#include <iostream>
#include <string>
#include <memory>
#include "FmtBrokerFactory.hpp"
#include "FmtBrokerMessage.hpp"

int main() {
    try {
        // Initialize the Kafka producer using the configuration file expected by the module.
        std::unique_ptr<FmtBrokerProducer> kafkaProducer = FmtBrokerFactory::createProducer("FmtBroker.cfg");

        // Build the ISO 8583 JSON payload using the dedicated formatter module.
        FmtBrokerMessage messageBuilder;
        messageBuilder.setMessageType("0210");
        messageBuilder.setPan("4111111111111111");
        messageBuilder.setProcessingCode("000000");
        messageBuilder.setAmount("000000010000"); // 100.00 formatted as cents.
        messageBuilder.setTransmissionDateTime("20240101123456");
        messageBuilder.setStan("123456");
        messageBuilder.setExpiry("2604");
        messageBuilder.setNsu("789012");
        messageBuilder.setResponseCode("00");
        messageBuilder.setAuthorizationCode("ABC123");
        const std::string jsonPayload = messageBuilder.buildJson();

        // Send the JSON payload to Kafka using the NSU as the partitioning key example.
        const std::string messageKey = "789012";
        kafkaProducer->send(messageKey, jsonPayload);
        kafkaProducer->flush();

        std::cout << "[SENT] key=" << messageKey
                  << " payload=" << jsonPayload << std::endl;
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "[ERROR] " << ex.what() << std::endl;
        return 1;
    }
}
