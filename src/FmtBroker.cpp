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
