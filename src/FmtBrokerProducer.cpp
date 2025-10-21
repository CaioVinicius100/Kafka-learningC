#include "FmtBrokerProducer.hpp"
#include <cppkafka/producer.h>
#include <cppkafka/message_builder.h>
#include <iostream>

FmtBrokerProducer::FmtBrokerProducer(const cppkafka::Configuration& cfg, const std::string& topic)
    : producer_{cfg}, topic_{topic} {}

void FmtBrokerProducer::send(const std::string& key, const std::string& payload) {
    cppkafka::MessageBuilder builder(topic_);
    builder.key(key);
    builder.payload(payload);
    try {
        producer_.produce(builder);
    } catch (const std::exception& ex) {
        std::cerr << "[PRODUCE_ERROR] " << ex.what() << std::endl;
        throw;
    }
}

void FmtBrokerProducer::flush() {
    producer_.flush();
}
