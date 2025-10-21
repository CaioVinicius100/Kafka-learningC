#include "FmtBrokerFactory.hpp"
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <stdexcept>
#include <map>

// Remove whitespace from both ends of the provided string.
static std::string trimWhitespace(const std::string& input) {
    std::size_t firstNonSpace = input.find_first_not_of(" \t\r\n");
    if (firstNonSpace == std::string::npos) {
        return "";
    }
    std::size_t lastNonSpace = input.find_last_not_of(" \t\r\n");
    return input.substr(firstNonSpace, lastNonSpace - firstNonSpace + 1);
}

cppkafka::Configuration FmtBrokerFactory::loadConfigurationFromFile(const std::string& path, std::string& topicNameOut) {
    std::ifstream inputFile(path);
    if (!inputFile) {
        throw std::runtime_error("Nao foi possivel abrir o arquivo de configuracao: " + path);
    }

    std::unordered_map<std::string, std::string> properties;
    std::string currentLine;
    while (std::getline(inputFile, currentLine)) {
        std::size_t commentPosition = currentLine.find('#');
        if (commentPosition != std::string::npos) {
            currentLine = currentLine.substr(0, commentPosition);
        }

        currentLine = trimWhitespace(currentLine);
        if (currentLine.empty()) {
            continue;
        }

        std::size_t equalsPosition = currentLine.find('=');
        if (equalsPosition == std::string::npos) {
            continue;
        }

        std::string key = trimWhitespace(currentLine.substr(0, equalsPosition));
        std::string value = trimWhitespace(currentLine.substr(equalsPosition + 1));
        if (!key.empty()) {
            properties[key] = value;
        }
    }

    if (!properties.count("bootstrap.servers")) {
        throw std::runtime_error("Config faltando: bootstrap.servers");
    }
    if (!properties.count("topic")) {
        throw std::runtime_error("Config faltando: topic");
    }

    topicNameOut = properties["topic"];
    properties.erase("topic");

    std::map<std::string, std::string> orderedConfiguration(properties.begin(), properties.end());
    return cppkafka::Configuration(orderedConfiguration);
}

std::unique_ptr<FmtBrokerProducer> FmtBrokerFactory::createProducer(const std::string& configurationPath) {
    std::string topicName;
    cppkafka::Configuration configuration = loadConfigurationFromFile(configurationPath, topicName);
    // Use the loaded configuration and topic name to construct the producer instance consumed by main().
    return std::unique_ptr<FmtBrokerProducer>(new FmtBrokerProducer(configuration, topicName));
}
