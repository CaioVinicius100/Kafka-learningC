#pragma once
#include <memory>
#include <string>
#include <cppkafka/cppkafka.h>
#include "FmtBrokerProducer.hpp"

class FmtBrokerFactory {
public:
    // Create a Kafka producer configured using the provided configuration file path.
    static std::unique_ptr<FmtBrokerProducer> createProducer(const std::string& configurationPath);

private:
    // Read the configuration file and return the Kafka configuration object while extracting the topic name.
    static cppkafka::Configuration loadConfigurationFromFile(const std::string& path, std::string& topicNameOut);
};
