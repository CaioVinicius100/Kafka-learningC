#include "FmtBrokerMessage.hpp"

#include <sstream>
#include <boost/property_tree/json_parser.hpp>

FmtBrokerMessage::FmtBrokerMessage()
    : messageType_(),
      pan_(),
      processingCode_(),
      amount_(),
      transmissionDateTime_(),
      stan_(),
      expiry_(),
      nsu_(),
      responseCode_(),
      authorizationCode_() {}

void FmtBrokerMessage::setMessageType(const std::string& value) {
    messageType_ = value;
}

void FmtBrokerMessage::setPan(const std::string& value) {
    pan_ = value;
}

void FmtBrokerMessage::setProcessingCode(const std::string& value) {
    processingCode_ = value;
}

void FmtBrokerMessage::setAmount(const std::string& value) {
    amount_ = value;
}

void FmtBrokerMessage::setTransmissionDateTime(const std::string& value) {
    transmissionDateTime_ = value;
}

void FmtBrokerMessage::setStan(const std::string& value) {
    stan_ = value;
}

void FmtBrokerMessage::setExpiry(const std::string& value) {
    expiry_ = value;
}

void FmtBrokerMessage::setNsu(const std::string& value) {
    nsu_ = value;
}

void FmtBrokerMessage::setResponseCode(const std::string& value) {
    responseCode_ = value;
}

void FmtBrokerMessage::setAuthorizationCode(const std::string& value) {
    authorizationCode_ = value;
}

std::string FmtBrokerMessage::buildJson() const {
    boost::property_tree::ptree root;
    root.put("mti", messageType_);
    root.put("pan", pan_);
    root.put("processing_code", processingCode_);
    root.put("amount", amount_);
    root.put("transmission_datetime", transmissionDateTime_);
    root.put("stan", stan_);
    root.put("expiry", expiry_);
    root.put("nsu", nsu_);
    root.put("response_code", responseCode_);
    root.put("authorization_code", authorizationCode_);

    std::ostringstream buffer;
    boost::property_tree::write_json(buffer, root, false);
    return buffer.str();
}
