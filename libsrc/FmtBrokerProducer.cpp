#include "FmtBrokerProducer.hpp"
#include <cppkafka/producer.h>
#include <cppkafka/message_builder.h>
#include <iostream>

// Construct the producer with the provided Kafka configuration and topic name.
FmtBrokerProducer::FmtBrokerProducer(const cppkafka::Configuration& cfg, const std::string& topic)
    : producer_{cfg}, topic_{topic} {}

// Publish a single message to Kafka using the provided key and payload text.
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

// Block until all queued messages are delivered or the configured timeout elapses.
void FmtBrokerProducer::flush() {
    producer_.flush();
}
