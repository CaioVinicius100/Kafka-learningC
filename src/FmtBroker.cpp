#include <iostream>
#include <stdexcept>
#include <thread>
#include <chrono>

#include "FmtBroker.hpp"
#include "FmtBrokerMessage.hpp"

// Demonstrates the call flow required by the BRDShcFinAuthResp transaction:
//   1. Build the JSON payload via FmtBrokerMessage.
//   2. Pass the generated string to FmtBroker::publish for a synchronous send.
//   3. Pass the same payload to FmtBroker::triggerAsync to use the asynchronous path.
int main() {
    try {
        fmtbroker::FmtBroker broker;
        broker.loadConfiguration("cfg/FmtBroker.cfg");

        FmtBrokerMessage message;
        message.setMessageType("0210");
        message.setPan("4111111111111111");
        message.setProcessingCode("000000");
        message.setAmount("000000010000");
        message.setTransmissionDateTime("20240101123456");
        message.setStan("123456");
        message.setExpiry("2604");
        message.setNsu("789012");
        message.setResponseCode("00");
        message.setAuthorizationCode("ABC123");
        const std::string jsonPayload = message.buildJson();

        broker.publish(jsonPayload);
        broker.triggerAsync(jsonPayload);

        // Give the detached asynchronous thread a moment to run in this example.
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } catch (const std::exception& ex) {
        std::cerr << "Erro ao enviar mensagem: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
