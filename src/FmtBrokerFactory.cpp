#include "FmtBrokerFactory.hpp"
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <algorithm>
#include <stdexcept>

static inline std::string trim(const std::string& s) {
    auto i = s.find_first_not_of(" \t\r\n");
    if (i == std::string::npos) return "";
    auto j = s.find_last_not_of(" \t\r\n");
    return s.substr(i, j - i + 1);
}

cppkafka::Configuration FmtBrokerFactory::load_config(const std::string& path, std::string& topic_out) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("Nao foi possivel abrir o arquivo de configuracao: " + path);

    std::unordered_map<std::string, std::string> props;
    std::string line;
    while (std::getline(in, line)) {
        auto hash = line.find('#');
        if (hash != std::string::npos) line = line.substr(0, hash);
        line = trim(line);
        if (line.empty()) continue;
        auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        auto key = trim(line.substr(0, eq));
        auto val = trim(line.substr(eq + 1));
        if (!key.empty()) props[key] = val;
    }

    if (!props.count("bootstrap.servers")) throw std::runtime_error("Config faltando: bootstrap.servers");
    if (!props.count("topic")) throw std::runtime_error("Config faltando: topic");
    topic_out = props["topic"];
    props.erase("topic");

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(props.size());
    for (auto& kv : props) kvs.emplace_back(kv.first, kv.second);
    return cppkafka::Configuration{kvs.begin(), kvs.end()};
}

std::unique_ptr<FmtBrokerProducer> FmtBrokerFactory::make_producer(const std::string& cfg_path) {
    std::string topic;
    auto cfg = load_config(cfg_path, topic);
    return std::unique_ptr<FmtBrokerProducer>(new FmtBrokerProducer(cfg, topic));
}
