#pragma once

#include <string>
#include <boost/property_tree/ptree.hpp>

// FmtBrokerMessage encapsulates the ISO 8583 data used to build the JSON payload
// forwarded by the Kafka producer. The setters map individual ISO fields to
// dedicated functions to keep the message construction explicit and easy to follow.
class FmtBrokerMessage {
public:
    FmtBrokerMessage();

    // Individual setters for the ISO 8583 fields handled by the formatter.
    void setMessageType(const std::string& messageType);
    void setPan(const std::string& primaryAccountNumber);
    void setProcessingCode(const std::string& processingCode);
    void setAmount(const std::string& transactionAmount);
    void setTransmissionDateTime(const std::string& transmissionDateTime);
    void setStan(const std::string& systemTraceAuditNumber);
    void setExpiry(const std::string& cardExpiryDate);
    void setNsu(const std::string& nsu);
    void setResponseCode(const std::string& responseCode);
    void setAuthorizationCode(const std::string& authorizationCode);

    // Build the final JSON representation expected by the downstream consumer.
    std::string buildJson() const;

private:
    boost::property_tree::ptree isoFields_;
};
