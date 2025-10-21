#pragma once
#include <memory>
#include <string>
#include <cppkafka/cppkafka.h>
#include "FmtBrokerProducer.hpp"

class FmtBrokerFactory {
public:
    static std::unique_ptr<FmtBrokerProducer> make_producer(const std::string& cfg_path);
private:
    static cppkafka::Configuration load_config(const std::string& path, std::string& topic_out);
};
