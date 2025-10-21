#include "FmtBrokerMessage.hpp"

#include <boost/property_tree/json_parser.hpp>
#include <sstream>

FmtBrokerMessage::FmtBrokerMessage() {}

void FmtBrokerMessage::setMessageType(const std::string& messageType) {
    isoFields_.put("iso8583.message_type", messageType);
}

void FmtBrokerMessage::setPan(const std::string& primaryAccountNumber) {
    isoFields_.put("iso8583.pan", primaryAccountNumber);
}

void FmtBrokerMessage::setProcessingCode(const std::string& processingCode) {
    isoFields_.put("iso8583.processing_code", processingCode);
}

void FmtBrokerMessage::setAmount(const std::string& transactionAmount) {
    isoFields_.put("iso8583.amount", transactionAmount);
}

void FmtBrokerMessage::setTransmissionDateTime(const std::string& transmissionDateTime) {
    isoFields_.put("iso8583.transmission_datetime", transmissionDateTime);
}

void FmtBrokerMessage::setStan(const std::string& systemTraceAuditNumber) {
    isoFields_.put("iso8583.stan", systemTraceAuditNumber);
}

void FmtBrokerMessage::setExpiry(const std::string& cardExpiryDate) {
    isoFields_.put("iso8583.expiry", cardExpiryDate);
}

void FmtBrokerMessage::setNsu(const std::string& nsu) {
    isoFields_.put("iso8583.nsu", nsu);
}

void FmtBrokerMessage::setResponseCode(const std::string& responseCode) {
    isoFields_.put("iso8583.response_code", responseCode);
}

void FmtBrokerMessage::setAuthorizationCode(const std::string& authorizationCode) {
    isoFields_.put("iso8583.authorization_code", authorizationCode);
}

std::string FmtBrokerMessage::buildJson() const {
    boost::property_tree::ptree root;
    root.put("source", "IST");
    root.put("channel", "FMT_BROKER");
    root.add_child("payload", isoFields_);

    std::ostringstream output;
    boost::property_tree::write_json(output, root, false);
    return output.str();
}
