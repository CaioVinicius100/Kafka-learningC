#include <iostream>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <string>
#include <memory>
#include "FmtBrokerFactory.hpp"

// Represents the transaction information that will be published to Kafka.
struct TransactionMessage {
    std::string networkName;            // VISA, ELO
    std::string messageTypeIndicator;   // 0200, 0210...
    std::string processingCode;
    std::string maskedPan;
    std::string authorizedAmount;       // ex: 000000001000 (R$10,00)
    std::string currencyCode;           // BRL
    std::string systemTraceAuditNumber; // DE11
    std::string retrievalReference;     // DE37
    std::string authorizationCode;      // DE38
    std::string responseCode;           // DE39
    std::string decisionOutcome;        // approved/declined/reversed/canceled
    std::string sourceModule;           // IST/SHC/YMRB
};

// Escape characters that are not valid inside JSON string values.
static std::string escapeJsonString(const std::string& input) {
    std::ostringstream escapedOutput;
    for (unsigned char character : input) {
        switch (character) {
            case '"': escapedOutput << "\\\""; break;
            case '\\': escapedOutput << "\\\\"; break;
            case '\b': escapedOutput << "\\b"; break;
            case '\f': escapedOutput << "\\f"; break;
            case '\n': escapedOutput << "\\n"; break;
            case '\r': escapedOutput << "\\r"; break;
            case '\t': escapedOutput << "\\t"; break;
            default:
                if (character < 0x20) {
                    escapedOutput << "\\u" << std::hex << std::setw(4) << std::setfill('0') << int(character);
                } else {
                    escapedOutput << character;
                }
        }
    }
    return escapedOutput.str();
}

// Produce the current UTC timestamp in ISO8601 format, e.g. 2024-01-01T12:34:56Z.
static std::string getCurrentTimestampIso8601() {
    std::time_t currentTime = std::time(nullptr);
    std::tm utcTime{};
#ifdef _WIN32
    gmtime_s(&utcTime, &currentTime);
#else
    gmtime_r(&currentTime, &utcTime);
#endif
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &utcTime);
    return buffer;
}

// Convert the transaction structure into the JSON payload expected by the Kafka consumer.
static std::string buildTransactionJson(const TransactionMessage& transaction) {
    std::ostringstream jsonBuilder;
    jsonBuilder << "{"
      << "\"timestamp\":\"" << getCurrentTimestampIso8601() << "\"," 
      << "\"source\":\"" << escapeJsonString(transaction.sourceModule) << "\"," 
      << "\"network\":\"" << escapeJsonString(transaction.networkName) << "\"," 
      << "\"mti\":\"" << escapeJsonString(transaction.messageTypeIndicator) << "\"," 
      << "\"processing_code\":\"" << escapeJsonString(transaction.processingCode) << "\"," 
      << "\"pan_masked\":\"" << escapeJsonString(transaction.maskedPan) << "\"," 
      << "\"amount\":\"" << escapeJsonString(transaction.authorizedAmount) << "\"," 
      << "\"currency\":\"" << escapeJsonString(transaction.currencyCode) << "\"," 
      << "\"stan\":\"" << escapeJsonString(transaction.systemTraceAuditNumber) << "\"," 
      << "\"rrn\":\"" << escapeJsonString(transaction.retrievalReference) << "\"," 
      << "\"auth_code\":\"" << escapeJsonString(transaction.authorizationCode) << "\"," 
      << "\"response_code\":\"" << escapeJsonString(transaction.responseCode) << "\"," 
      << "\"decision\":\"" << escapeJsonString(transaction.decisionOutcome) << "\""
      << "}";
    return jsonBuilder.str();
}

int main() {
    try {
        // Initialize the Kafka producer using the configuration file expected by the module.
        std::unique_ptr<FmtBrokerProducer> kafkaProducer = FmtBrokerFactory::createProducer("FmtBroker.cfg");

        // Example transaction that demonstrates the format expected by the downstream systems.
        TransactionMessage sampleTransaction {
            "VISA",
            "0210",
            "000000",
            "411111******1111",
            "000000001000",
            "BRL",
            "123456",
            "123456789012",
            "654321",
            "00",
            "approved",
            "IST"
        };

        // Serialize the transaction to JSON and send it as a Kafka message.
        std::string jsonPayload = buildTransactionJson(sampleTransaction);
        kafkaProducer->send(sampleTransaction.systemTraceAuditNumber, jsonPayload);
        kafkaProducer->flush();

        std::cout << "[SENT] key=" << sampleTransaction.systemTraceAuditNumber
                  << " payload=" << jsonPayload << std::endl;
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "[ERROR] " << ex.what() << std::endl;
        return 1;
    }
}
