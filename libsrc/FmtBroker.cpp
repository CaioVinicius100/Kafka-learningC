#include "FmtBroker.hpp"

#include <sstream>
#include <stdexcept>

#include "ConfigFileBR.hpp"
#include "utility.h"

namespace fmtbroker {

FmtBroker::FmtBroker() : configuration_(), producer_(), topic_(), producerMutex_() {}

FmtBroker::~FmtBroker() {
    std::lock_guard<std::mutex> lock(producerMutex_);
    if (producer_) {
        producer_->flush();
    }
}

void FmtBroker::loadConfiguration(const std::string& cfgPath) {
    ConfigFileBR config;
    config.openFile(cfgPath);

    std::string bootstrap;
    if (!config.getSingleParam("bootstrap.servers", bootstrap)) {
        throw std::runtime_error("FmtBroker: parametro bootstrap.servers ausente em " + cfgPath);
    }
    std::string topic;
    if (!config.getSingleParam("topic", topic)) {
        throw std::runtime_error("FmtBroker: parametro topic ausente em " + cfgPath);
    }

    std::string clientId;
    if (!config.getSingleParam("client.id", clientId)) {
        clientId = "fmtbroker-producer";
    }

    configuration_.set("bootstrap.servers", bootstrap);
    configuration_.set("client.id", clientId);

    std::string value;
    if (config.getSingleParam("acks", value)) {
        configuration_.set("acks", value);
    }
    if (config.getSingleParam("enable.idempotence", value)) {
        configuration_.set("enable.idempotence", value);
    }
    if (config.getSingleParam("linger.ms", value)) {
        configuration_.set("linger.ms", value);
    }
    if (config.getSingleParam("security.protocol", value)) {
        configuration_.set("security.protocol", value);
    }
    if (config.getSingleParam("sasl.mechanisms", value)) {
        configuration_.set("sasl.mechanisms", value);
    }
    if (config.getSingleParam("sasl.username", value)) {
        configuration_.set("sasl.username", value);
    }
    if (config.getSingleParam("sasl.password", value)) {
        configuration_.set("sasl.password", value);
    }
    if (config.getSingleParam("ssl.ca.location", value)) {
        configuration_.set("ssl.ca.location", value);
    }

    topic_ = topic;
    config.closeFile();
}

void FmtBroker::publish(const std::string& payload) {
    ensureProducer();
    std::lock_guard<std::mutex> lock(producerMutex_);
    cppkafka::MessageBuilder builder(topic_);
    builder.payload(payload);
    producer_->produce(builder);
    producer_->flush();
}

void FmtBroker::triggerAsync(const std::string& payload) {
    ThreadContext context;
    context.instance = this;
    context.payload = payload;
    std::thread threadObject(threadEntryPublish, context);
    threadObject.detach();
}

void FmtBroker::threadEntryPublish(ThreadContext context) {
    try {
        context.instance->publish(context.payload);
    } catch (...) {
        // The caller already handles logging; swallow exceptions to avoid terminating the thread.
    }
}

void FmtBroker::ensureProducer() {
    std::lock_guard<std::mutex> lock(producerMutex_);
    if (!producer_) {
        producer_.reset(new cppkafka::Producer(configuration_));
    }
}

} // namespace fmtbroker
