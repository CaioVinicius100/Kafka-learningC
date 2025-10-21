#pragma once
#include <string>
#include <cppkafka/cppkafka.h>

class FmtBrokerProducer {
public:
    FmtBrokerProducer(const cppkafka::Configuration& cfg, const std::string& topic);
    void send(const std::string& key, const std::string& payload);
    void flush();
private:
    cppkafka::Producer producer_;
    std::string topic_;
};
