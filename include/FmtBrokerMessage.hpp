#ifndef FMTBROKERMESSAGE_HPP
#define FMTBROKERMESSAGE_HPP

#include <string>
#include <boost/property_tree/ptree.hpp>

// FmtBrokerMessage is responsible exclusively for building the JSON payload that will be
// published to Kafka. Each setter stores the individual field that composes the final
// ISO8583-derived document.  The buildJson method marshals the internal state into a
// string using Boost.PropertyTree; no Kafka logic lives in this module.
class FmtBrokerMessage {
public:
    FmtBrokerMessage();

    // Set the MTI of the ISO 8583 message (e.g. 0210).
    void setMessageType(const std::string& value);

    // Set the primary account number (PAN) of the card holder.
    void setPan(const std::string& value);

    // Set the ISO 8583 processing code.
    void setProcessingCode(const std::string& value);

    // Set the transaction amount encoded in cents.
    void setAmount(const std::string& value);

    // Set the transmission date-time (field 7).
    void setTransmissionDateTime(const std::string& value);

    // Set the system trace audit number (STAN).
    void setStan(const std::string& value);

    // Set the card expiration date (YYMM format).
    void setExpiry(const std::string& value);

    // Set the network sequence number (NSU).
    void setNsu(const std::string& value);

    // Set the response code returned by the authorizer.
    void setResponseCode(const std::string& value);

    // Set the authorization code returned by the authorizer.
    void setAuthorizationCode(const std::string& value);

    // Compile all stored fields into a JSON string using Boost.PropertyTree.
    std::string buildJson() const;

private:
    std::string messageType_;
    std::string pan_;
    std::string processingCode_;
    std::string amount_;
    std::string transmissionDateTime_;
    std::string stan_;
    std::string expiry_;
    std::string nsu_;
    std::string responseCode_;
    std::string authorizationCode_;
};

#endif // FMTBROKERMESSAGE_HPP
