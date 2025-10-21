# Kafka-learningC

## Call flow overview
1. The business logic (e.g., `BRDShcFinAuthResp`) populates `FmtBrokerMessage` using the field level
   setters (MTI, PAN, processing code, etc.).
2. `FmtBrokerMessage::buildJson()` returns the final JSON payload exactly as expected by the
   downstream consumer.
3. The caller hands the JSON string to `fmtbroker::FmtBroker::publish` (synchronous) or
   `fmtbroker::FmtBroker::triggerAsync` (asynchronous thread entry) to send the message to Kafka.

## Required configuration keys (`cfg/FmtBroker.cfg`)
- `bootstrap.servers`
- `topic`
- Optional: `client.id`, `acks`, `enable.idempotence`, `linger.ms`, `security.protocol`,
  `sasl.mechanisms`, `sasl.username`, `sasl.password`, `ssl.ca.location`.

## Building
The project targets GCC 8.2+ with `-std=c++14`.  Ensure Boost (PropertyTree) and cppkafka
are available and expose headers through `$(BOOST_ROOT)` / `$(KAFKA_HOME)` as configured in the
Makefiles. Link against `librdkafka`, `librdkafka++`, and the IST platform libraries already
listed in the `Makefile.am` files.

## Triggering the asynchronous path
Use `fmtbroker::FmtBroker::triggerAsync(jsonPayload)`. The method spawns a `std::thread`
(using the static `threadEntryPublish` helper) that publishes the payload without blocking the caller.
