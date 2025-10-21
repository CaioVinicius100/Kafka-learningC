#include <iostream>
#include <sstream>
#include <iomanip>
#include <ctime>
#include "FmtBrokerFactory.hpp"

struct Txn {
    std::string network;       // VISA, ELO
    std::string mti;           // 0200, 0210...
    std::string processing_code;
    std::string pan_masked;
    std::string amount;        // ex: 000000001000 (R$10,00)
    std::string currency;      // BRL
    std::string stan;          // DE11
    std::string rrn;           // DE37
    std::string auth_code;     // DE38
    std::string resp_code;     // DE39
    std::string decision;      // approved/declined/reversed/canceled
    std::string source;        // IST/SHC/YMRB
};

static std::string escape_json(const std::string& s) {
    std::ostringstream o;
    for (unsigned char c : s) {
        switch (c) {
            case '"': o << "\""; break;
            case '\\': o << "\\\"; break;
            case '\b': o << "\\b"; break;
            case '\f': o << "\\f"; break;
            case '\n': o << "\\n"; break;
            case '\r': o << "\\r"; break;
            case '\t': o << "\\t"; break;
            default:
                if (c < 0x20) { o << "\\u" << std::hex << std::setw(4) << std::setfill('0') << int(c); }
                else { o << c; }
        }
    }
    return o.str();
}

static std::string now_iso8601() {
    std::time_t now = std::time(nullptr);
    std::tm tm{};
    #ifdef _WIN32
    gmtime_s(&tm, &now);
    #else
    gmtime_r(&now, &tm);
    #endif
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return buf;
}

static std::string to_json(const Txn& t) {
    std::ostringstream j;
    j << "{"
      << "\"timestamp\":\"" << now_iso8601() << "\","
      << "\"source\":\"" << escape_json(t.source) << "\","
      << "\"network\":\"" << escape_json(t.network) << "\","
      << "\"mti\":\"" << escape_json(t.mti) << "\","
      << "\"processing_code\":\"" << escape_json(t.processing_code) << "\","
      << "\"pan_masked\":\"" << escape_json(t.pan_masked) << "\","
      << "\"amount\":\"" << escape_json(t.amount) << "\","
      << "\"currency\":\"" << escape_json(t.currency) << "\","
      << "\"stan\":\"" << escape_json(t.stan) << "\","
      << "\"rrn\":\"" << escape_json(t.rrn) << "\","
      << "\"auth_code\":\"" << escape_json(t.auth_code) << "\","
      << "\"response_code\":\"" << escape_json(t.resp_code) << "\","
      << "\"decision\":\"" << escape_json(t.decision) << "\""
      << "}";
    return j.str();
}

int main() {
    try {
        auto producer = FmtBrokerFactory::make_producer("FmtBroker.cfg");

        Txn tx {
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

        std::string payload = to_json(tx);
        producer->send(tx.stan, payload);
        producer->flush();

        std::cout << "[SENT] key=" << tx.stan << " payload=" << payload << std::endl;
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "[ERROR] " << ex.what() << std::endl;
        return 1;
    }
}
