#include <iostream>
#include <string>
#include <memory>
#include "FmtBrokerFactory.hpp"

int main() {
    try {
        // Initialize the Kafka producer using the configuration file expected by the module.
        std::unique_ptr<FmtBrokerProducer> kafkaProducer = FmtBrokerFactory::createProducer("FmtBroker.cfg");

        // In the production environment the FmtBroker module builds the JSON payload using Boost.
        // This sample uses a static example representing that output to illustrate the integration.
        const std::string messageKey = "123456"; // Example STAN or identifier used for partitioning.
        const std::string fmtBrokerPayload =
            R"({"timestamp":"2024-01-01T12:34:56Z","source":"IST","network":"VISA","mti":"0210","response_code":"00"})";

        // Send the pre-built JSON payload coming from FmtBroker to the Kafka topic configured in FmtBroker.cfg.
        kafkaProducer->send(messageKey, fmtBrokerPayload);
        kafkaProducer->flush();

        std::cout << "[SENT] key=" << messageKey
                  << " payload=" << fmtBrokerPayload << std::endl;
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "[ERROR] " << ex.what() << std::endl;
        return 1;
    }
}
