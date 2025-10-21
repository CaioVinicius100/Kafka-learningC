#ifndef FMTBROKER_HPP
#define FMTBROKER_HPP

#include <string>
#include <memory>
#include <thread>
#include <mutex>

#include <cppkafka/configuration.h>
#include <cppkafka/producer.h>

namespace fmtbroker {

// FmtBroker owns the Kafka producer used by the application. It is responsible for
// loading the producer configuration, creating the cppkafka::Producer instance, and
// publishing JSON payloads produced by FmtBrokerMessage.
class FmtBroker {
public:
    FmtBroker();
    ~FmtBroker();

    // Load configuration values from cfgPath (uses existing Util helpers internally).
    void loadConfiguration(const std::string& cfgPath);

    // Publish the provided JSON payload synchronously to the configured Kafka topic.
    void publish(const std::string& payload);

    // Spawn a detached thread that will publish the JSON payload asynchronously.
    void triggerAsync(const std::string& payload);

private:
    struct ThreadContext {
        FmtBroker* instance;
        std::string payload;
    };

    static void threadEntryPublish(ThreadContext context);

    void ensureProducer();

    cppkafka::Configuration configuration_;
    std::unique_ptr<cppkafka::Producer> producer_;
    std::string topic_;
    std::mutex producerMutex_;
};

} // namespace fmtbroker

#endif // FMTBROKER_HPP
