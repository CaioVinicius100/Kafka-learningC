#include <iostream>
#include <string>
#include <memory>
#include "FmtBrokerFactory.hpp"

int main() {
    try {
        // Initialize the Kafka producer using the configuration file expected by the module.
        std::unique_ptr<FmtBrokerProducer> kafkaProducer = FmtBrokerFactory::createProducer("FmtBroker.cfg");
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "[ERROR] " << ex.what() << std::endl;
        return 1;
    }
}
