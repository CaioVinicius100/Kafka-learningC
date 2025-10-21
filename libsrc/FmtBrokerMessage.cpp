#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE 1
#endif

#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wdeprecated"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
// STD
#include <chrono>
#include <time.h>
#include <algorithm>
#include <iomanip>
#include <cstring>
#include <optional>
#include <cctype>
#include <cstdio>
#include "segmentos.h"

// IST libs
#include <tm.h>
#include <shc.h>
#include <dbm.h>
#include <shcfmt.h>
#include <ist_otrace.h>
#include <ist_gps_misc.h>
#include <ShcAppBase.h>
#include <tokenizer.h>
#include "mb.h"

// LOCAL
#include "FmtBrokerMessage.hpp"
#include "ConfigFileBR.hpp"
#include "ShcmsgLogger.hpp"
#include "utility.h"
#include "Iso8583ParserBase.hpp"
#include <string>
#include <utility>
#include <ist_seg_elf_id_map.h>
#include <ist_seg_name.h>
#include <ist_seg_id.h>
#include <ist_seg_network_settlement_data.h>

// JSON
#include <boost/json.hpp>
#include <boost/algorithm/string.hpp>

// Kafka
#include <cppkafka/cppkafka.h>

namespace
{
    constexpr const char *kDefaultContentType = "application/json";
}

// Static member definition
unsigned short FmtBrokerMessage::mSegmentIdNwSetData = 0;
FmtBrokerMessage::~FmtBrokerMessage()
{
    OTraceInfo("~FmtBrokerMessage()--------------\n");
    try
    {
        if (m_producer)
            m_producer->flush(std::chrono::milliseconds(3000));
    }
    catch (...)
    {
    }
}

inline bool iequals(const std::string &a, const std::string &b)
{
    return std::equal(
        a.begin(), a.end(),
        b.begin(), b.end(),
        [](char a, char b)
        {
            return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
        });
}

void FmtBrokerMessage::init()
{
    postInit();
}

void FmtBrokerMessage::exec()
{
    OTraceInfo("FmtBrokerMessage::exec - no-op (aguardando eventos externos)\n");
}

// Construtor
FmtBrokerMessage::FmtBrokerMessage() : MailboxBaseBR()
{
    OTraceInfo("FmtBrokerMessage()\n");

    mFmtName = "fmtbroker/1.0.0";
    mServiceName = "";
    mTransactionName = "";
    mBootstrapServers = "";
    mTopic = "";
    mTokenEnable = false;
    mTokenExpiryTime = "";
    mTokenHostService = "";
    mTokenUrl = "";
    mTokenClientId = "";
    mTokenClientSecret = "";
    mSecondsToExpire = 0;
    mServiceContentType = "";
    mTokenContentType = "";
    mSendIso8583 = true;
}

void FmtBrokerMessage::postInit()
{
    OTraceDebug("FmtBrokerMessage::postInit\n");

    // Arquivo de configuração
    ConfigFileBR lConfig;
    lConfig.openFile(getFmtName() + ".cfg");

    // serviceContentType / header.content-type
    if (!lConfig.getSingleParam("header.content-type", mServiceContentType))
    {
        if (!lConfig.getSingleParam("serviceContentType", mServiceContentType))
        {
            mServiceContentType = "application/json";
        }
    }
    // trim
    boost::algorithm::trim(mServiceContentType);

    // tokenContentType
    if (!lConfig.getSingleParam("tokenContentType", mTokenContentType))
    {
        mTokenContentType = "application/json";
    }
    // trim
    boost::algorithm::trim(mTokenContentType);

    // bootstrap.servers
    if (!lConfig.getSingleParam("bootstrap.servers", mBootstrapServers))
    {
        std::string erro = "Erro ao tentar encontrar o parametro 'bootstrap.servers'";
        syslg(erro.c_str());
        OTraceFatal(erro.c_str());
        throw std::runtime_error(erro);
    }
    // trim
    boost::algorithm::trim(mBootstrapServers);

    // topic
    if (!lConfig.getSingleParam("topic", mTopic))
    {
        std::string erro = "Erro ao tentar encontrar o parametro 'topic'";
        syslg(erro.c_str());
        OTraceFatal(erro.c_str());
        throw std::runtime_error(erro);
    }
    boost::algorithm::trim(mTopic);

    // tokenEnable
    std::string lTokenEnable = "off";
    if (!lConfig.getSingleParam("tokenEnable", lTokenEnable))
    {
        std::string erro = "Erro ao tentar encontrar o parametro 'tokenEnable'";
        syslg(erro.c_str());
        OTraceFatal(erro.c_str());
        throw std::runtime_error(erro);
    }
    boost::algorithm::trim(lTokenEnable);

    if (boost::iequals(lTokenEnable, "on"))
    {
        mTokenEnable = true;
    }

    // tokenExpiryTime
    if (!lConfig.getSingleParam("tokenExpiryTime", mTokenExpiryTime))
    {
        mTokenExpiryTime = "3600";
    }
    boost::algorithm::trim(mTokenExpiryTime);

    // tokenHostService
    if (!lConfig.getSingleParam("tokenHostService", mTokenHostService))
    {
        mTokenHostService = "";
    }
    boost::algorithm::trim(mTokenHostService);

    // tokenUrl
    if (!lConfig.getSingleParam("tokenUrl", mTokenUrl))
    {
        mTokenUrl = "";
    }
    boost::algorithm::trim(mTokenUrl);

    // client.id / tokenClientId fallback
    if (!lConfig.getSingleParam("client.id", mTokenClientId))
    {
        if (!lConfig.getSingleParam("tokenClientId", mTokenClientId))
        {
            std::string erro = "Erro ao tentar encontrar o parametro 'client.id' ou 'tokenClientId'";
            syslg(erro.c_str());
            OTraceFatal(erro.c_str());
            throw std::runtime_error(erro);
        }
    }
    boost::algorithm::trim(mTokenClientId);

    // tokenClientSecret
    if (!lConfig.getSingleParam("tokenClientSecret", mTokenClientSecret))
    {
        mTokenClientSecret = "";
    }
    boost::algorithm::trim(mTokenClientSecret);

    // sendIso8583
    std::string lSendIso8583 = "";
    if (!lConfig.getSingleParam("sendIso8583", lSendIso8583))
    {
        lSendIso8583 = "on";
    }

    // NETWORK_SETTLEMENT_DATA id
    mSegmentIdNwSetData = ElfIdMap::elf_to_id(IST_SEG_NETWORK_SETTLEMENT_DATA);

    // ISO 8583 parsers
    mSendIso8583 = true;
    if (lSendIso8583 == "off")
    {
        mSendIso8583 = false;
    }
    parseVisa.init();
    parseElo.init();

    // Kafka Producer (inline)
    cppkafka::Configuration conf;
    struct ParamInfo
    {
        std::string key;
        std::string value;
        std::string origin;
    };
    std::vector<ParamInfo> applied;
    applied.reserve(24);
    auto mark = [&](const std::string &k, const std::string &v, const std::string &o)
    { applied.push_back({k, v, o}); };

    auto trimInPlace = [](std::string &s)
    { 
        boost::algorithm::trim(s); 
    };
    
    auto readTrim = [&](const char *k, std::string &dst) -> bool
    {
        if(!lConfig.getSingleParam(k, dst)) return false; trimInPlace(dst); return !dst.empty(); 
    };

    auto setIfCfg = [&](const char *k)
    { 
        std::string v; if(readTrim(k,v)){ conf.set(k,v); mark(k,v,"cfg"); 
        } 
    };

    auto setDefault = [&](const char *k, const char *v)
    { 
        conf.set(k,v); mark(k,v,"default"); 
    };

    // Obrigatórios
    conf.set("bootstrap.servers", mBootstrapServers);
    mark("bootstrap.servers", mBootstrapServers, "cfg");
    conf.set("client.id", mTokenClientId);
    mark("client.id", mTokenClientId, "cfg");

    // Opcionais
    setIfCfg("acks");
    setIfCfg("retries");
    setIfCfg("batch.size");
    setIfCfg("linger.ms");
    setIfCfg("security.protocol");

    // Idempotência
    {
        std::string v;
        if (readTrim("enable.idempotence", v))
        {
            conf.set("enable.idempotence", v);
            mark("enable.idempotence", v, "cfg");
        }
        else
        {
            setDefault("enable.idempotence", "true");
        }
    }
    // Defaults de idempotência se ausentes
    auto ensureDefaultIfMissing = [&](const char* k, const char* v){
        bool already = false;
        for (size_t i=0;i<applied.size();++i) if (applied[i].key == k) { already = true; break; }
        if (!already) setDefault(k, v);
    };
    ensureDefaultIfMissing("acks", "all");
    ensureDefaultIfMissing("retries", "10");
    ensureDefaultIfMissing("max.in.flight.requests.per.connection", "5");

    // SSL / SASL
    setIfCfg("ssl.ca.location");
    std::string mech;
    bool mechCfg = false;
    if (readTrim("sasl.mechanisms", mech))
    {
        conf.set("sasl.mechanisms", mech);
        mark("sasl.mechanisms", mech, "cfg");
        mechCfg = true;
    }
    if (!mechCfg && readTrim("sasl.mechanism", mech))
    {
        conf.set("sasl.mechanisms", mech);
        mark("sasl.mechanisms", mech, "cfg(alias)");
        mechCfg = true;
    }
    setIfCfg("sasl.username");
    setIfCfg("sasl.password");

    // Callbacks
    conf.set_delivery_report_callback([](cppkafka::Producer &, const cppkafka::Message &m)
                                      {
        if (m.get_error()) {
            OTraceError("Kafka DR FAIL topic=%s key=%.*s err=%s\n", m.get_topic().c_str(), (int)m.get_key().get_size(), m.get_key().get_data() ? (const char*)m.get_key().get_data() : "", m.get_error().to_string().c_str());
        } else {
            OTraceDebug("Kafka DR OK topic=%s key=%.*s partition=%d offset=%lld\n", m.get_topic().c_str(), (int)m.get_key().get_size(), m.get_key().get_data() ? (const char*)m.get_key().get_data() : "", m.get_partition(), (long long)m.get_offset());
        } });
    conf.set_error_callback([](cppkafka::KafkaHandleBase &, int err, const std::string &reason)
                            {
        if (err) OTraceError("Kafka ERROR code=%d reason=%s\n", err, reason.c_str()); else OTraceDebug("Kafka notice: %s\n", reason.c_str()); });
    conf.set_log_callback([](cppkafka::KafkaHandleBase &, int level, const std::string &facility, const std::string &message)
                          { if (level <= 3) OTraceDebug("Kafka log[%d][%s]: %s\n", level, facility.c_str(), message.c_str()); });

    m_kafkaConfig = std::move(conf);
    bool legacyOk = false;
    try {
        m_producer = std::make_unique<cppkafka::Producer>(m_kafkaConfig);
        legacyOk = true;
    } catch (const std::exception &ex) {
        OTraceError("Falha criando Kafka Producer legado: %s\n", ex.what());
    }

    // Tentativa de inicializar wrapper simplificado (FmtBrokerProducer) a partir do mesmo arquivo cfg
    fmtbroker::FmtBrokerConfig cfg;
    std::string cfgErr;
    if (cfg.loadFromFile(getFmtName() + ".cfg", &cfgErr)) {
        m_simpleProducer.reset(new fmtbroker::FmtBrokerProducer());
        std::string initErr;
        if (m_simpleProducer->init(cfg, &initErr)) {
            m_simpleProducerInit = true;
            OTraceInfo("FmtBrokerMessage: wrapper FmtBrokerProducer inicializado (topic=%s).\n", cfg.getTopic().c_str());
        } else {
            OTraceWarning("FmtBrokerMessage: falha init FmtBrokerProducer (%s) - mantendo apenas caminho legado.\n", initErr.c_str());
            m_simpleProducer.reset();
        }
    } else {
        OTraceDebug("FmtBrokerMessage: não carregou FmtBrokerConfig (%s) - prossegue só com producer legado.\n", cfgErr.c_str());
    }

    if (!legacyOk && !m_simpleProducerInit) {
        OTraceFatal("FmtBrokerMessage: nenhum producer Kafka inicializado. Abortando.\n");
        throw std::runtime_error("Kafka producer init failed");
    }

    // Consistência idempotência x acks
    bool idempotenceOn = false; std::string acksValue;
    for (size_t i=0;i<applied.size();++i) {
        if (applied[i].key == "enable.idempotence" && (applied[i].value == "true" || applied[i].value == "1")) idempotenceOn = true;
        if (applied[i].key == "acks") acksValue = applied[i].value;
    }
    if (idempotenceOn && !acksValue.empty() && acksValue != "all" && acksValue != "-1") {
        OTraceError("Kafka config inconsistente: enable.idempotence=true mas acks=%s (esperado 'all'). Reavalie %s.cfg.\n", acksValue.c_str(), getFmtName().c_str());
    }

    // Sumário dos parâmetros Kafka
    OTraceDebug("--- Kafka Producer Params (origem) ---\n");
    for (size_t i = 0; i < applied.size(); ++i)
    {
        OTraceDebug("  %-35s = %-20s (%s)\n", applied[i].key.c_str(), applied[i].value.c_str(), applied[i].origin.c_str());
    }
    OTraceDebug("---------------------------------------\n");

    lConfig.closeFile();

    // Dump final de parâmetros
    OTraceDebug("========== PARAMETROS LIDOS (FmtBrokerMessage): ==========\n");
    OTraceDebug("  serviceContentType.......:[%s]\n", mServiceContentType.c_str());
    OTraceDebug("  tokenContentType.........:[%s]\n", mTokenContentType.c_str());
    OTraceDebug("  bootstrap.servers........:[%s]\n", mBootstrapServers.c_str());
    OTraceDebug("  topic...................:[%s]\n", mTopic.c_str());
    OTraceDebug("  tokenEnable..............:[%d][%s]\n", mTokenEnable, lTokenEnable.c_str());
    OTraceDebug("  tokenExpiryTime..........:[%s]\n", mTokenExpiryTime.c_str());
    OTraceDebug("  tokenHostService.........:[%s]\n", mTokenHostService.c_str());
    OTraceDebug("  tokenUrl.................:[%s]\n", mTokenUrl.c_str());
    OTraceDebug("  client.id................:[%s]\n", mTokenClientId.c_str());
    OTraceDebug("  tokenClientSecret........:[%s]\n", mTokenClientSecret.c_str());
    OTraceDebug("  sendIso8583..............:[%s]\n", lSendIso8583.c_str());
    OTraceDebug("=======================================\n");
}

void FmtBrokerMessage::postClearMsg()
{
    OTraceDebug("FmtBrokerMessage::postClearMsg\n");
    mServiceName = "";
}

bool FmtBrokerMessage::getTransactionName(ShcmsgHeader *msg)
{
    char lBuffer[128] = {0};
    int lDataLen = 0;

    memset(lBuffer, 0, sizeof(lBuffer));

    msg->getSegmentData(lBuffer, &lDataLen, ElfIdMap::elf_to_id("FBR_TRN_DATA"), 1); // FBR_NOME_TRN = 1
    std::string lNomeTransacao = lBuffer;
    OTraceDebug("seg.FBR_TRN_DATA.FBR_NOME_TRN=[%s]\n", lNomeTransacao.c_str());

    mTransactionName = "";

    if (lDataLen > 1)
    {
        boost::algorithm::trim(lNomeTransacao);
        mTransactionName = lNomeTransacao;
        return true;
    }

    return false;
}

bool FmtBrokerMessage::getServiceName(ShcmsgHeader *msg)
{
    if (getTransactionName(msg))
    {
        if ((mTransactionName.compare(VEND_DBT) == 0) || (mTransactionName.compare(ADV_VEND_DBT) == 0) || (mTransactionName.compare(SAQUE_DBT) == 0) || (mTransactionName.compare(ADV_SAQUE_DBT) == 0) || (mTransactionName.compare(REEMB_COMP_DBT) == 0) || (mTransactionName.compare(ADV_REEMB_COMP_DBT) == 0) || (mTransactionName.compare(ACC_FDG_DBT) == 0) || (mTransactionName.compare(ADV_ACC_FDG_DBT) == 0) || (mTransactionName.compare(ORIG_CRD_DBT) == 0) || (mTransactionName.compare(ADV_ORIG_CRD_DBT) == 0) || (mTransactionName.compare(PREAUT_DBT) == 0))
        {
            mServiceName = VEND_DBT;
        }
        else if ((mTransactionName.compare(EST_VEND_DBT) == 0) || (mTransactionName.compare(DESF_EST_VEND_DBT) == 0) || (mTransactionName.compare(EST_SAQUE_DBT) == 0) || (mTransactionName.compare(DESF_EST_SAQUE_DBT) == 0) || (mTransactionName.compare(EST_REEMB_COMP_DBT) == 0) || (mTransactionName.compare(DESF_EST_REEMB_COMP_DBT) == 0) || (mTransactionName.compare(EST_ACC_FDG_DBT) == 0) || (mTransactionName.compare(DESF_EST_ACC_FDG_DBT) == 0) || (mTransactionName.compare(EST_ORIG_CRD_DBT) == 0) || (mTransactionName.compare(DESV_EST_ORIG_CRD_DBT) == 0))
        {
            mServiceName = EST_VEND_DBT;
        }
    }

    OTraceDebug("TransactionName=[%s]\n", mTransactionName.c_str());
    OTraceDebug("ServiceName=[%s]\n", mServiceName.c_str());

    return !mServiceName.empty();
}

bool FmtBrokerMessage::indicadorReversaoParcial(ShcmsgHeader *msg)
{
    double lAmountValue = 0.0;
    lAmountValue = dbm_dectodbl(&msg->msg.new_amount);

    if (lAmountValue > 0.0)
    {
        return true;
    }

    return false;
}

bool FmtBrokerMessage::isVendaDebito()
{
    if (strcmp(mServiceName.c_str(), VEND_DBT) == 0)
    {
        return true;
    }

    return false;
}

bool FmtBrokerMessage::isReversaoVendaDebito()
{
    if (strcmp(mServiceName.c_str(), EST_VEND_DBT) == 0)
    {
        return true;
    }

    return false;
}

bool FmtBrokerMessage::isAdviceVendaDebito()
{
    if ((mTransactionName.compare(ADV_VEND_DBT) == 0) || (mTransactionName.compare(ADV_SAQUE_DBT) == 0) || (mTransactionName.compare(ADV_REEMB_COMP_DBT) == 0) || (mTransactionName.compare(ADV_ACC_FDG_DBT) == 0) || (mTransactionName.compare(ADV_ORIG_CRD_DBT) == 0))
    {
        return true;
    }

    return false;
}

bool FmtBrokerMessage::isVendaDebitoCachBack(ShcmsgHeader *msg)
{
    double lAmountValue = 0.0;
    lAmountValue = dbm_dectodbl(&msg->msg.cash_back);

    if (lAmountValue > 0.0)
    {
        return true;
    }

    return false;
}

bool FmtBrokerMessage::isAccountFunding()
{
    if ((mTransactionName.compare(ACC_FDG_DBT) == 0) ||
        (mTransactionName.compare(ADV_ACC_FDG_DBT) == 0) ||
        (mTransactionName.compare(EST_ACC_FDG_DBT) == 0) ||
        (mTransactionName.compare(DESF_EST_ACC_FDG_DBT) == 0))
    {
        return true;
    }

    return false;
}

std::string FmtBrokerMessage::getKafkaBootstrapServers()
{
    return mBootstrapServers;
}

std::string FmtBrokerMessage::getKafkaTopic()
{
    return mTopic;
}

bool FmtBrokerMessage::tokenEnable()
{
    OTraceDebug("FmtBrokerMessage::tokenEnable\n");

    return mTokenEnable;
}

void FmtBrokerMessage::processPreRequest(ShcmsgHeader *msg, json::object &jsonObject)
{
    OTraceDebug("FmtBrokerMessage::processPreRequest\n");

    if (tokenEnable())
    {
        if (!tokenIsValid())
        {
            // obtem novo token
            getNewToken();
        }
    }
}

// Monta JSON e publica no Kafka
void FmtBrokerMessage::processTransaction(ShcmsgHeader *msg)
{
    if (!msg)
        return;

    // Determina se deve processar a transação (lógica simplificada)
    std::string reason = "transaction_processed";
    if (msg->msg.respcode != 0)
    {
        reason = "transaction_denied";
    }

    json::object payload;
    buildJsonRequest(msg, payload);
    payload["publishReason"] = reason;

    // Publica no Kafka
    sendToKafka(msg, payload);

    OTraceInfo("FmtBrokerMessage publish reason=[%s] trace=%06d topic=%s rc=%d\n",
               reason.c_str(), msg->msg.trace, mTopic.c_str(), msg->msg.respcode);
}

// PAN/Track/EMV sempre mascarados.
void FmtBrokerMessage::buildJsonRequest(ShcmsgHeader *msg, json::object &jsonObject)
{
    OTraceDebug("FmtBrokerMessage::buildJsonRequest\n");

    Utility::getSegmentSwCartao(msg, m_sw_cartao);

    formatCodigoIdTransacao(msg, jsonObject);
    formatCanal(msg, jsonObject);
    formatAplicativoOrigem(msg, jsonObject);

    json::object jsonObjectTerminal;
    formatIdentificacao(msg, jsonObjectTerminal);
    formatGeolocalizacao(msg, jsonObjectTerminal);

    json::object jsonObjectCaracteristicas;
    // formatModoEntrada      ( msg, jsonObjectCaracteristicas);
    formatTipo(msg, jsonObjectCaracteristicas);
    formatInstalacao(msg, jsonObjectCaracteristicas);

    jsonObjectTerminal["caracteristicas"] = jsonObjectCaracteristicas;
    jsonObject["terminal"] = jsonObjectTerminal;

    json::object jsonObjectDadosCliente;
    formatTipoCliente(msg, jsonObjectDadosCliente);
    json::object jsonObjectDocumento;
    formatTipoDocumento(msg, jsonObjectDocumento);
    formatNumero(msg, jsonObjectDocumento);
    jsonObjectDadosCliente["documento"] = jsonObjectDocumento;

    formatSegmento(msg, jsonObjectDadosCliente);
    formatTelefone(msg, jsonObjectDadosCliente);
    jsonObject["dadosCliente"] = jsonObjectDadosCliente;

    json::object jsonObjectDadosConta;
    formatAgencia(msg, jsonObjectDadosConta);
    formatConta(msg, jsonObjectDadosConta);
    formatDigito(msg, jsonObjectDadosConta);
    formatTitularidade(msg, jsonObjectDadosConta);
    jsonObject["dadosConta"] = jsonObjectDadosConta;

    json::object jsonObjectDadosCartao;
    formatNumeroCartao(msg, jsonObjectDadosCartao);
    formatDtValidadeCartao(msg, jsonObjectDadosCartao);
    formatTipoCartao(msg, jsonObjectDadosCartao);
    formatTipoBandeira(msg, jsonObjectDadosCartao);
    formatTitularidadeCt(msg, jsonObjectDadosCartao);
    formatSegmentoProduto(msg, jsonObjectDadosCartao);
    jsonObject["dadosCartao"] = jsonObjectDadosCartao;

    json::object jsonObjectDadosTransacao;
    json::object jsonObjectOperacao;
    formatModoEntradaOperacao(msg, jsonObjectOperacao);
    // formatIndicadorAtualizarSaldo   ( msg, jsonObjectOperacao );
    formatIndicadorSenha(msg, jsonObjectOperacao);
    formatIndicadorCvv2(msg, jsonObjectOperacao);
    formatTipoDebitoCredito(msg, jsonObjectOperacao);
    formatIndicadorParcelado(msg, jsonObjectOperacao);
    jsonObjectDadosTransacao["operacao"] = jsonObjectOperacao;

    json::object jsonObjectEstabelecimento;
    formatCodigoEstabelecimento(msg, jsonObjectEstabelecimento);
    formatCodigoPostal(msg, jsonObjectEstabelecimento);
    formatCidadeEstabelecimento(msg, jsonObjectEstabelecimento);
    // formatUf                        ( msg, jsonObjectEstabelecimento );
    formatPais(msg, jsonObjectEstabelecimento);
    formatIsoPais(msg, jsonObjectEstabelecimento);
    formatSetorAtividade(msg, jsonObjectEstabelecimento);
    jsonObjectDadosTransacao["estabelecimento"] = jsonObjectEstabelecimento;

    formatDataHora(msg, jsonObjectDadosTransacao);
    formatDataHoraOperacional(msg, jsonObjectDadosTransacao);
    // formatDataHoraProcessamento( msg, jsonObjectDadosTransacao );
    // formatDataDocumento        ( msg, jsonObjectDadosTransacao );
    formatTipoTransacao(msg, jsonObjectDadosTransacao);
    formatEtapaTransacao(msg, jsonObjectDadosTransacao);
    // formatProtocolo            ( msg, jsonObjectDadosTransacao );
    // formatTelefonePrepago      ( msg, jsonObjectDadosTransacao );
    // formatBancoAdquirente      ( msg, jsonObjectDadosTransacao );
    formatCodigoAdquirente(msg, jsonObjectDadosTransacao);
    formatMoedaTransacao(msg, jsonObjectDadosTransacao);
    formatValorTransacao(msg, jsonObjectDadosTransacao);
    formatCodigoProcessamento(msg, jsonObjectDadosTransacao);
    // formatOrigemLimite         ( msg, jsonObjectDadosTransacao );
    // formatLimiteOperacao       ( msg, jsonObjectDadosTransacao );
    formatTipoMensagem(msg, jsonObjectDadosTransacao);
    formatTipoOperacao(msg, jsonObjectDadosTransacao);
    // formatIdMaquina                ( msg, jsonObjectDadosTransacao );
    // formatControleSessao           ( msg, jsonObjectDadosTransacao );
    // formatSistemaOperacional       ( msg, jsonObjectDadosTransacao );
    // formatNavegador                ( msg, jsonObjectDadosTransacao );
    // formatIndicadorMaquinaCadastrada ( msg, jsonObjectDadosTransacao );
    formatIndicadorTokenizado(msg, jsonObjectDadosTransacao);
    formatExpiracaoToken(msg, jsonObjectDadosTransacao);
    formatSolicitanteToken(msg, jsonObjectDadosTransacao);
    formatScoreBandeira(msg, jsonObjectDadosTransacao);
    formatScoreAdquirente(msg, jsonObjectDadosTransacao);
    formatCodigoResposta(msg, jsonObjectDadosTransacao);
    formatCodigoRespostaExtendido(msg, jsonObjectDadosTransacao);
    formatTipoFraude(msg, jsonObjectDadosTransacao);

    formatIndicadorEcommerce(msg, jsonObjectDadosTransacao);
    formatCodigoSegFatorAut(msg, jsonObjectDadosTransacao);
    formatAtcCartao(msg, jsonObjectDadosTransacao);
    formatAtcIST(msg, jsonObjectDadosTransacao);
    formatIndicadorCarteiraDigital(msg, jsonObjectDadosTransacao);

    jsonObject["dadosTransacao"] = jsonObjectDadosTransacao;

    if (mSendIso8583)
    {
        formatIso8583(msg, jsonObject);
    }
}

// Envio ao Kafka: key = codigoIdTransacao ou trace; adiciona header content-type.
void FmtBrokerMessage::sendToKafka(ShcmsgHeader *msg, json::object &jsonObject)
{
    if (mTopic.empty())
    {
        OTraceError("FmtBrokerMessage::sendToKafka: tópico Kafka não configurado\n");
        return;
    }

    // Deriva a key
    std::string key;
    if (jsonObject.contains("codigoIdTransacao") && jsonObject["codigoIdTransacao"].is_string())
    {
        key = boost::json::value_to<std::string>(jsonObject["codigoIdTransacao"]);
    }
    else
    {
        char buf[16] = {0};
        std::snprintf(buf, sizeof(buf), "%06d", msg->msg.trace);
        key = buf;
    }

    // Serializa JSON
    const std::string payload = boost::json::serialize(boost::json::value(jsonObject));

    // Monta headers Kafka (content-type)
    cppkafka::MessageBuilder builder(mTopic);
    builder.key(key).payload(payload);
    // Header content-type para alinhamento com consumidores genéricos.
    const std::string &ct = mServiceContentType.empty() ? std::string("application/json") : mServiceContentType;
    builder.header({"content-type", ct});

    // Caminho preferencial: usar FmtBrokerProducer simplificado
    if (!m_simpleProducerInit)
    {
        // FmtBrokerConfig mínimo a partir das propriedades já conhecidas.
        fmtbroker::FmtBrokerConfig cfg;
        std::string err;
        // Estratégia: se existir um arquivo padrão "FmtBroker.cfg" tentamos carregar, senão injetamos somente bootstrap/topic.
        bool loaded = false;
        if (cfg.loadFromFile("FmtBroker.cfg", &err)) {
            loaded = true;
        } else {
            // Fallback manual: precisamos pelo menos de bootstrap.servers, topic e message.timeout.ms p/ validação.
            // Como FmtBrokerConfig não expõe set direto, este fallback é limitado; se falhar, usamos m_producer legado.
        }
        if (loaded) {
            // Confere se o tópico do config bate com mTopic; se não, seguimos com mTopic mesmo (override via produce topicOverride).
            m_simpleProducer.reset(new fmtbroker::FmtBrokerProducer());
            if (!m_simpleProducer->init(cfg, &err)) {
                OTraceWarning("FmtBrokerMessage::sendToKafka: init FmtBrokerProducer falhou (%s) - usando caminho legado direto.\n", err.c_str());
                m_simpleProducer.reset();
            } else {
                m_simpleProducerInit = true;
            }
        }
    }

    if (m_simpleProducerInit && m_simpleProducer) {
        // Headers extras (já adicionamos content-type antes no builder local; aqui apenas replicamos)
        std::map<std::string, std::string> extraHeaders; // vazio pois content-type é gerenciado internamente
        std::string err;
        if (!m_simpleProducer->produce(payload, key, extraHeaders, mTopic, &err)) {
            OTraceError("FmtBrokerMessage::sendToKafka: falha produce via FmtBrokerProducer: %s\n", err.c_str());
        } else {
            m_simpleProducer->poll(0);
            OTraceDebug("FmtBrokerMessage::sendToKafka(simple) enviado topic=%s key=%s trace=%06d\n", mTopic.c_str(), key.c_str(), msg->msg.trace);
        }
        return; // não continua para o caminho legado
    }

    // Caminho legado (m_producer direto) se wrapper não pôde inicializar
    if (!m_producer) {
        OTraceError("FmtBrokerMessage::sendToKafka: producer não inicializado (caminho legado)\n");
        return;
    }
    try {
        m_producer->produce(builder);
        size_t outq = (size_t)m_producer->get_out_queue_length();
        m_producer->poll(std::chrono::milliseconds(outq > 100 ? 5 : 0));
        OTraceDebug("FmtBrokerMessage::sendToKafka - mensagem enviada (legado): topic=[%s], key=[%s], trace=%06d\n", mTopic.c_str(), key.c_str(), msg->msg.trace);
    } catch (const std::exception &ex) {
        OTraceError("FmtBrokerMessage::sendToKafka exceção (legado): %s\n", ex.what());
    }
}

// === Identificadores / Canal ===
// Campos de identificação primária da transação e origem (canal, app).
void FmtBrokerMessage::formatCodigoIdTransacao(ShcmsgHeader *msg, json::object &jsonObject)
{
    char buffer[256] = {0};
    char buffer2[256] = {0};
    int len = -1;

    // Chargeback? tenta SW_PAN_REFNUM_CHARGEBACK
    if (isChargeback(msg))
    {
        msg->getSegmentData(buffer2, &len, ElfIdMap::elf_to_id("SW_PAN_REFNUM_CHARGEBACK"), 1);
    }
    if (len <= 0)
    {
        // fallback: MC_NETWORK_DATA_REFNUM em NETWORK_SETTLEMENT_DATA
        len = 0;
        std::memset(buffer2, 0, sizeof(buffer2));
#ifdef MC_NETWORK_DATA_REFNUM
        msg->getSegmentData(buffer2, &len, ElfIdMap::elf_to_id(NETWORK_SETTLEMENT_DATA), MC_NETWORK_DATA_REFNUM);
#endif
    }
    if (len > 0)
    {
        if (std::strlen(buffer2) < 12)
        {
            std::snprintf(buffer, std::strlen(buffer2) + 1, "%s", buffer2);
        }
        else
        {
            std::snprintf(buffer, 12 + 1, "%s", buffer2);
        }
    }
    else
    {
        // Zera o nsuIST para enviar ao Event Broker sem expor conteúdo aleatório
        std::memset(buffer, '0', 12);
        buffer[12] = '\0';
    }
    jsonObject["codigoIdTransacao"] = std::string(buffer);
}

void FmtBrokerMessage::formatCanal(ShcmsgHeader *msg, json::object &jsonObject)
{
    // Fixo conforme referência
    jsonObject["canal"] = 66;
}

void FmtBrokerMessage::formatAplicativoOrigem(ShcmsgHeader *msg, json::object &jsonObject)
{
    // Fixo "IST" conforme referência
    jsonObject["aplicativoOrigem"] = "IST";
}

// === Terminal ===
// Dados do terminal físico/lógico e suas características.
void FmtBrokerMessage::formatIdentificacao(ShcmsgHeader *msg, json::object &jsonObject)
{
    std::string lBuffer(msg->msg.termid);
    boost::algorithm::trim_right(lBuffer);
    jsonObject["identificacao"] = lBuffer;
}

void FmtBrokerMessage::formatGeolocalizacao(ShcmsgHeader *msg, json::object &jsonObject)
{
    char geo[60 + 1] = {0};
    if (Utility::isELO(msg))
    {
        char buf[1024] = {0};
        int len = 0;
        msg->getSegmentData(buf, &len, ElfIdMap::elf_to_id("POS_DATA"), 0);
        if (len > 0)
            std::memcpy(geo, &buf[0], static_cast<size_t>(std::min(len, 60)));
    }
    else if (Utility::isVisa(msg))
    {
        if (std::strlen(msg->msg.pos_geo_loc) > 0)
        {
            std::snprintf(geo, sizeof(geo), "%.13s", msg->msg.pos_geo_loc);
        }
    }
    if (std::strlen(geo) > 0)
        jsonObject["geolocalizacao"] = std::string(geo);
}

void FmtBroker::formatTipo(ShcmsgHeader *msg, json::object &jsonObject)
{
    std::string tipo = "26";
    if (msg->msg.merchant_type == 6011)
    {
        tipo = "02";
    }
    else if (isFromECommerce(msg))
    {
        tipo = "14";
    }
    jsonObject["tipo"] = std::move(tipo);
}

void FmtBroker::formatInstalacao(ShcmsgHeader *msg, json::object &jsonObject)
{
    unsigned char segbuf[2048] = {0};
    int seglen = 0;
    char out[2] = {0};

    if (Utility::isELO(msg))
    {
        // DE060.3 (indicador de localização do terminal)
        msg->getSegmentData(reinterpret_cast<char *>(&segbuf[0]), &seglen, ElfIdMap::elf_to_id("DE060_ELO"), 0);
        if (seglen >= 3)
            out[0] = static_cast<char>(segbuf[2]);
    }
    if (out[0] != '\0')
    {
        jsonObject["instalacao"] = std::string(1, out[0]);
    }
}

// === Cliente / Documento / Contato ===
// Dados cadastrais essenciais do cliente e contato.
void FmtBroker::formatTipoCliente(ShcmsgHeader *msg, json::object &jsonObject)
{
    if (m_sw_cartao.ID_TIPO_TITULAR_CARTAO == 'F')
        jsonObject["tipo"] = "PF";
    else if (m_sw_cartao.ID_TIPO_TITULAR_CARTAO == 'J')
        jsonObject["tipo"] = "PJ";
}

void FmtBroker::formatTipoDocumento(ShcmsgHeader *msg, json::object &jsonObject)
{
    if (m_sw_cartao.ID_TIPO_TITULAR_CARTAO == 'J')
        jsonObject["tipo"] = "CNPJ";
    else if (m_sw_cartao.ID_TIPO_TITULAR_CARTAO == 'F')
        jsonObject["tipo"] = "CPF";
}

void FmtBroker::formatNumero(ShcmsgHeader *msg, json::object &jsonObject)
{
    const std::string v = m_sw_cartao.ID_TITULAR_CARTAO;
    if (!v.empty())
        jsonObject["numero"] = v;
}

void FmtBroker::formatSegmento(ShcmsgHeader *msg, json::object &jsonObject)
{
    const std::string v = m_sw_cartao.SEGMENTO;
    if (!v.empty())
        jsonObject["segmento"] = std::atoi(v.c_str());
}

void FmtBroker::formatTelefone(ShcmsgHeader *msg, json::object &jsonObject)
{
    char tel[16] = {0};
    dbm_dectochar_conv(&m_sw_cartao.TELEFONE, tel, 0);
    if (std::strlen(tel) > 0)
        jsonObject["telefone"] = std::string(tel);
}

// === Conta ===
// Identificação de agência, conta e titularidade.
void FmtBroker::formatAgencia(ShcmsgHeader *msg, json::object &jsonObject)
{
    jsonObject["agencia"] = m_sw_cartao.CODIGO_AGENCIA;
}

void FmtBroker::formatConta(ShcmsgHeader *msg, json::object &jsonObject)
{
    char buf[24] = {0};
    dbm_dectochar_conv(&m_sw_cartao.CODIGO_CONTA, buf, 0);
    // strtrim(buf, buf); // se necessário no seu utilitário
    if (std::strlen(buf) > 0)
        jsonObject["conta"] = std::string(buf);
}

void FmtBroker::formatDigito(ShcmsgHeader *msg, json::object &jsonObject)
{
    char buf[24] = {0};
    dbm_dectochar_conv(&m_sw_cartao.CODIGO_CONTA, buf, 0);
    const size_t n = std::strlen(buf);
    if (n > 0)
    {
        jsonObject["digito"] = std::string(1, buf[n - 1]);
    }
}

void FmtBroker::formatTitularidade(ShcmsgHeader *msg, json::object &jsonObject)
{
    if (m_sw_cartao.TITULAR_CARTAO != '\0' && m_sw_cartao.TITULAR_CARTAO != ' ')
    {
        jsonObject["titularidade"] = std::string(1, m_sw_cartao.TITULAR_CARTAO);
    }
}

// === Cartão ===
// Sempre mascarar PAN/trilhas/EMV; somente metadados de tipo/bandeira.
void FmtBroker::formatNumeroCartao(ShcmsgHeader *msg, json::object &jsonObject)
{
    char buffer[24] = {0};

    strtrim(msg->msg.pan, buffer);

    if (strlen(buffer) > 0)
    {
        std::string pan(buffer);
        // Mascaramento: manter BIN (6) + últimos 4; resto '*'.
        if (pan.size() > 10) {
            std::string masked = pan;
            size_t keepStart = 6;
            size_t keepEnd = 4;
            if (masked.size() > keepStart + keepEnd) {
                for (size_t i = keepStart; i < masked.size() - keepEnd; ++i) masked[i] = '*';
            }
            jsonObject["numero"] = masked;
        } else {
            // Curto demais: mascara tudo exceto primeiro e último se existir
            std::string masked = pan;
            if (masked.size() > 2) {
                for (size_t i=1;i<masked.size()-1;++i) masked[i] = '*';
            } else if (masked.size() == 2) {
                masked[1] = '*';
            }
            jsonObject["numero"] = masked;
        }
    }
}

void FmtBroker::formatDtValidadeCartao(ShcmsgHeader *msg, json::object &jsonObject)
{
    char out[8] = {0};
    auto *shc_data_p = reinterpret_cast<struct shc_data *>(msg->msg.shc_data_buffer);

    if (shc_data_p && std::strlen(shc_data_p->expiry_date) >= 4)
    {
        // YYMM
        std::string expiry{shc_data_p->expiry_date};
        int year = std::atoi(expiry.substr(0, 2).c_str());
        int mon = std::atoi(expiry.substr(2, 2).c_str());
        if (mon > 0 && mon <= 12)
        {
            std::snprintf(out, sizeof(out), "%02d%02d", year, mon);
        }
    }
    else if (msg->msg.track2 && std::strlen(msg->msg.track2) > 0)
    {
        std::string trk{msg->msg.track2};
        auto pos = trk.find('=');
        if (pos == std::string::npos)
            pos = trk.find('D');
        if (pos != std::string::npos && trk.size() >= pos + 5)
        {
            int yy = std::atoi(trk.substr(pos + 1, 2).c_str());
            int mm = std::atoi(trk.substr(pos + 3, 2).c_str());
            std::snprintf(out, sizeof(out), "%02d%02d", yy, mm);
        }
    }
    if (std::strlen(out) > 0)
    {
        jsonObject["validade"] = std::string(out);
    }
}

void FmtBroker::formatTipoCartao(ShcmsgHeader *msg, json::object &jsonObject)
{
    // Fisico/Virtual, conforme referência
    jsonObject["tipo"] = (m_sw_cartao.ID_CARTAO_VIRTUAL == 'S') ? "Virtual" : "Fisico";
}

void FmtBroker::formatTipoBandeira(ShcmsgHeader *msg, json::object &jsonObject)
{
    if (Utility::isELO(msg))
        jsonObject["bandeira"] = "Elo";
    else if (Utility::isVisa(msg))
        jsonObject["bandeira"] = "Visa";
}

void FmtBroker::formatTitularidadeCt(ShcmsgHeader *msg, json::object &jsonObject)
{
    if (m_sw_cartao.TITULAR_CARTAO != '\0' && m_sw_cartao.TITULAR_CARTAO != ' ')
    {
        jsonObject["titularidade"] = std::string(1, m_sw_cartao.TITULAR_CARTAO);
    }
}

void FmtBroker::formatSegmentoProduto(ShcmsgHeader *msg, json::object &jsonObject)
{
    const std::string v = m_sw_cartao.SEGMENTO;
    if (!v.empty())
        jsonObject["segmentoProduto"] = v;
}

// === Operação / Estabelecimento / Datas / Tipos ===
// Parâmetros da operação financeira, merchant e temporalidade.
void FmtBroker::formatModoEntradaOperacao(ShcmsgHeader *msg, json::object &jsonObject)
{
    char buffer[256] = {0};
    char buffer2[256] = {0};
    int Len = 0;

    memset(buffer, 0, sizeof(buffer));
    memset(buffer2, 0, sizeof(buffer2));

    if (Utility::isVisa(msg))
    {
        msg->getSegmentData(buffer2, &Len, ElfIdMap::elf_to_id(IST_SEG_NETWORK_SETTLEMENT_DATA), POS_ENTRY_CODE);
        OTraceDebug("========== MSG.SEGMENT.NETWORK_SETTLEMENT_DATA.POS_ENTRY_CODE: ==========\n");
        OTraceDebug("  Length:[%d]\n", Len);
        OTraceDebug("  Data:[%s]\n", buffer2);
        OTraceDebug("=========================================================================\n");

        // if( strtrim( buffer2, buffer2 ) > 0 )
        //{
        //     msg->msg.shift_number = atoi( buffer2 );
        // }

        strtrim(buffer2, buffer2);

        if (strlen(buffer2) == 0)
        {
            memset(buffer2, '\0', sizeof(buffer2));
            msg->getSegmentData(buffer2, &Len, ElfIdMap::elf_to_id(IST_SEG_NETWORK_SETTLEMENT_DATA), AcquirerOrigPcode);
        }
    }
    else if (Utility::isELO(msg))
    {
        sprintf(buffer2, "%03d", msg->msg.pos_entry_code);
    }
    else
    {
        OTraceDebug("ERRO: Issuer nao suportado! networkid[%s]\n", msg->msg.acquirer);
    }

    OTraceDebug("formatModoEntradaOperacao POS_ENTRY_CODE[%s]\nmsg->msg.pos_entry_code[%d]", buffer2, msg->msg.pos_entry_code);

    if (strlen(buffer2) > 0)
    {
        sprintf(buffer, "%03d", atoi(buffer2));
    }

    std::string modoEntrada(buffer);
    if (modoEntrada.length() > 2)
    {
        modoEntrada = modoEntrada.substr(0, 2);
    }

    if (modoEntrada.length() > 0)
    {
        jsonObject["modoEntrada"] = modoEntrada;
    }
}

void FmtBroker::formatIndicadorSenha(ShcmsgHeader *msg, json::object &jsonObject)
{
    // Nunca enviar PIN; apenas indicativo S/N
    jsonObject["indicadorSenha"] = (msg->msg.pin[0] != '\0') ? "S" : "N";
}

void FmtBroker::formatIndicadorCvv2(ShcmsgHeader *msg, json::object &jsonObject)
{
    char ind = 0;
    if (Utility::isELO(msg))
    {
        char buf[256] = {0};
        int len = 0;
        msg->getSegmentData(buf, &len, ElfIdMap::elf_to_id("DE126_ELO"), 0);
        ind = buf[0];
    }
    else if (Utility::isVisa(msg))
    {
        auto *shc = reinterpret_cast<struct shc_data *>(msg->msg.shc_data_buffer);
        if (shc && shc->cvv2_indicator != '3')
            ind = shc->cvv2_indicator;
    }
    jsonObject["indicadorCvv2"] = (ind == '1') ? "S" : "N";
}

void FmtBroker::formatTipoDebitoCredito(ShcmsgHeader *msg, json::object &jsonObject)
{
    jsonObject["tipoDebitoCredito"] = "Debito";
}

void FmtBroker::formatIndicadorParcelado(ShcmsgHeader *msg, json::object &jsonObject)
{
    jsonObject["indicadorParcelado"] = 0;
}

void FmtBroker::formatCodigoEstabelecimento(ShcmsgHeader *msg, json::object &jsonObject)
{
    char buf[24] = {0};
    std::snprintf(buf, 16, "%s", msg->msg.termloc);
    // strtrim(buf, buf);
    if (std::strlen(buf) > 0)
        jsonObject["codigo"] = std::string(buf);
}

void FmtBroker::formatCodigoPostal(ShcmsgHeader *msg, json::object &jsonObject)
{
    char postal[10 + 1] = {0};
    if (Utility::isELO(msg))
    {
        char lbuf[1024] = {0};
        int len = 0;
        msg->getSegmentData(lbuf, &len, ElfIdMap::elf_to_id("POS_DATA"), 0);
        if (len > 0)
            std::memcpy(postal, &lbuf[22], 9);
    }
    else if (Utility::isVisa(msg))
    {
        if (std::strlen(msg->msg.pos_geo_loc) > 5)
        {
            std::snprintf(postal, sizeof(postal), "%.8s", msg->msg.pos_geo_loc + 5);
        }
    }
    if (std::strlen(postal) > 0)
        jsonObject["codigoPostal"] = std::string(postal);
}

void FmtBroker::formatCidadeEstabelecimento(ShcmsgHeader *msg, json::object &jsonObject)
{
    char buffer[16] = {0};
    char city[13 + 1] = {0};

    memset(buffer, 0, sizeof(buffer));
    memset(city, 0, sizeof(city));

    if (Utility::isELO(msg)) // DE043
    {
        char bufElo[256] = {0};
        strcpy(bufElo, msg->msg.acceptorname);

        OTraceDebug("  acceptorname[%s]\n", msg->msg.acceptorname);

        snprintf(city, 13 + 1, "%13s", bufElo + 23);
        OTraceDebug("  Subfield City[%s]\n", city);
    }
    else if (Utility::isVisa(msg))
    {
        char buffer2[256] = {0};
        int Len = 0;

        msg->getSegmentData(buffer2, &Len, ElfIdMap::elf_to_id(IST_SEG_NETWORK_SETTLEMENT_DATA), ACCEPTOR_NAME);
        OTraceDebug("========= MSG.SEGMENT.NETWORK_SETTLEMENT_DATA.ACCEPTOR_NAME: ==========\n");
        OTraceDebug("  Length:[%d]\n", Len);
        OTraceDebug("  Data:[%s]\n", buffer2);
        if (Len >= 83) // Esse 83 ocorre porque é colocado no segmento 40 posicoes de buffer
                       //     para cada um dos itens (nome, cidade, pais) e garante que havera
                       //     ao menos preenchimento total dos 3 campos lidos.
        {
            snprintf(city, 13 + 1, "%13s", buffer2 + 40);
            OTraceDebug("  Subfield City[%s]\n", city);
        }
    }

    if (strlen(city) > 0)
    {
        sprintf(buffer, "%13s", city);
        strtrim(buffer, buffer);
        jsonObject["cidade"] = std::string(buffer);
    }
}
void FmtBroker::formatPais(ShcmsgHeader *msg, json::object &jsonObject)
{
    char buffer[8] = {0};
    char country[3 + 1] = {0};

    memset(buffer, 0, sizeof(buffer));
    memset(country, 0, sizeof(country));

    if (Utility::isELO(msg)) // DE043
    {
        char bufElo[256] = {0};
        strcpy(bufElo, msg->msg.acceptorname);

        OTraceDebug("  acceptorname[%s]\n", msg->msg.acceptorname);

        snprintf(country, 3 + 1, "%3s", bufElo + 23 + 14);
        OTraceDebug("  Subfield Country[%s]\n", country);
    }
    else if (Utility::isVisa(msg))
    {
        char buffer2[256] = {0};
        int Len = 0;

        msg->getSegmentData(buffer2, &Len, ElfIdMap::elf_to_id(IST_SEG_NETWORK_SETTLEMENT_DATA), ACCEPTOR_NAME);
        OTraceDebug("========= MSG.SEGMENT.NETWORK_SETTLEMENT_DATA.ACCEPTOR_NAME: ==========\n");
        OTraceDebug("  Length:[%d]\n", Len);
        OTraceDebug("  Data:[%s]\n", buffer2);
        if (Len >= 83) // Esse 83 ocorre porque é colocado no segmento 40 posicoes de buffer
                       //     para cada um dos itens (nome, cidade, pais) e garante que havera
                       //     ao menos preenchimento total dos 3 campos lidos.
        {
            snprintf(country, 3 + 1, "%3s", buffer2 + 80);
            OTraceDebug("  Subfield Country[%s]\n", country);
        }
    }

    if (strlen(country) > 0)
    {
        sprintf(buffer, "%3s", country);
        strtrim(buffer, buffer);
        jsonObject["pais"] = std::string(buffer);
    }
}

void FmtBroker::formatIsoPais(ShcmsgHeader *msg, json::object &jsonObject)
{
    if (msg->msg.acq_country > 0)
    {
        char buf[4] = {0};
        std::snprintf(buf, sizeof(buf), "%03d", msg->msg.acq_country);
        jsonObject["isoPais"] = std::string(buf);
    }
}

void FmtBroker::formatSetorAtividade(ShcmsgHeader *msg, json::object &jsonObject)
{
    if (msg->msg.merchant_type > 0)
    {
        char buf[8] = {0};
        std::snprintf(buf, sizeof(buf), "%04d", msg->msg.merchant_type);
        jsonObject["setorAtividade"] = std::string(buf);
    }
}

void FmtBroker::formatDataHora(ShcmsgHeader *msg, json::object &jsonObject)
{
    char buf[24] = {0};
    int day = msg->msg.local_date % 100;
    int mon = (msg->msg.local_date / 100) % 100;
    int year = msg->msg.local_date / 10000;
    int hour = msg->msg.local_time / 10000;
    int min = (msg->msg.local_time / 100) % 100;
    int sec = msg->msg.local_time % 100;
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d", year, mon, day, hour, min, sec);
    jsonObject["dataHora"] = std::string(buf);
}

void FmtBroker::formatDataHoraOperacional(ShcmsgHeader *msg, json::object &jsonObject)
{
    char buffer[16] = {0};
    long data;
    long hora;

    shc_get_current_date();
    data = shc_local_currentdate();
    hora = shc_local_currenttime();

    sprintf(buffer, "%08ld%06ld", data, hora);

    jsonObject["dataHoraOperacional"] = std::string(buffer);
}

void FmtBroker::formatTipoTransacao(ShcmsgHeader *msg, json::object &jsonObject)
{
    // A definir conforme seu domínio (mantido 0 como referência)
    jsonObject["tipoTransacao"] = 0;
}

void FmtBroker::formatEtapaTransacao(ShcmsgHeader *msg, json::object &jsonObject)
{
    int etapa = 0;
    char seg[128] = {0};
    int len = 0;
    msg->getSegmentData(seg, &len, ElfIdMap::elf_to_id("DE033_ELO"), 0);
    std::string nomeTrn = seg;

    if (!nomeTrn.empty() && (nomeTrn.rfind("DESF_", 0) == 0 || nomeTrn.rfind("EST_", 0) == 0))
    {
        etapa = 5; // cancelamento/reversões
    }
    else if (nomeTrn == "VERIF_CONTA_DBT" || nomeTrn == "CONS_VER_CONTA_DBT_AVS" || nomeTrn == "CONS_VER_CONTA_DBT_CVV")
    {
        etapa = 1; // intenção/verificação de conta
    }
    else
    {
        etapa = 2; // pagamento/compra
    }
    if (etapa > 0)
        jsonObject["etapaTransacao"] = etapa;
}

void FmtBroker::formatCodigoAdquirente(ShcmsgHeader *msg, json::object &jsonObject)
{
    char buf[16] = {0};
    int len = 0;
    if (Utility::isELO(msg))
    {
        msg->getSegmentData(buf, &len, ElfIdMap::elf_to_id("DE033_ELO"), 0);
    }
    else if (Utility::isVisa(msg))
    {
        std::strcpy(buf, msg->msg.F_ID);
    }
    if (std::strlen(buf) > 0)
        jsonObject["codigoAdquirente"] = std::string(buf);
}

void FmtBroker::formatMoedaTransacao(ShcmsgHeader *msg, json::object &jsonObject)
{
    if (msg->msg.acq_currency_code > 0)
        jsonObject["moedaTransacao"] = msg->msg.acq_currency_code;
}

void FmtBroker::formatValorTransacao(ShcmsgHeader *msg, json::object &jsonObject)
{
    char buf[24] = {0};
    dbm_dectochar_conv(&msg->msg.amount, buf, 2);
    jsonObject["valorTransacao"] = std::string(buf);
}

void FmtBroker::formatCodigoProcessamento(ShcmsgHeader *msg, json::object &jsonObject)
{
    // pcode como inteiro (igual referência que migrou para inteiro)
    jsonObject["codigoProcessamento"] = msg->msg.pcode;
}

void FmtBroker::formatTipoMensagem(ShcmsgHeader *msg, json::object &jsonObject)
{
    // Fixo "OP" (igual referência)
    jsonObject["tipoMensagem"] = "OP";
}

void FmtBroker::formatTipoOperacao(ShcmsgHeader *msg, json::object &jsonObject)
{
    // Referência usa origmsg
    jsonObject["tipoOperacao"] = msg->msg.origmsg;
}

// === Indicadores / Token / Scores / Resposta ===
// Sinais de risco/fraude, tokenização, scoring e códigos de retorno.
void FmtBroker::formatIndicadorTokenizado(ShcmsgHeader *msg, json::object &jsonObject)
{
    char segBuffer[1024] = {0};
    char retGetTag[128] = {0};
    int segLen = 0;
    std::string indicadorTokenizado("N");

    memset(segBuffer, 0, sizeof(segBuffer));
    memset(retGetTag, 0, sizeof(retGetTag));

    if (Utility::isELO(msg))
    {
        // DE106 - Conjunto 61 - Tag 2 (ID do requisitante do token)
        msg->getSegmentData(segBuffer, &segLen, ElfIdMap::elf_to_id("DE106_ELO"), 0);

        if (Utility::getTagConjDadosDe106(segBuffer, segLen, 61, 2, retGetTag))
        {
            indicadorTokenizado = "S";
        }
    }
    else if (Utility::isVisa(msg))
    {
        // DE123 Dataset 0x68 TAG 0x03 (Token Requestor ID)
        msg->getSegmentData(segBuffer, &segLen, ElfIdMap::elf_to_id(IST_SEG_NETWORK_SETTLEMENT_DATA), TOKEN_REQUESTER_ID);

        if (segLen > 0)
        {
            indicadorTokenizado = "S";
        }
    }

    if (indicadorTokenizado.length() > 0)
    {
        jsonObject["indicadorTokenizado"] = indicadorTokenizado;
    }
}

void FmtBroker::formatExpiracaoToken(ShcmsgHeader *msg, json::object &jsonObject)
{
    // TODO campo no formato N-8 ou AN-8 ?
    // A VALIDAR COM BRADESCO
    int tamDe = 0;
    char tokenExp[16] = {0};

    memset(tokenExp, 0, sizeof(tokenExp));

    if (Utility::isELO(msg))
    {
        char de106[1024] = {0};
        char retGetTag[128] = {0};

        memset(de106, 0, sizeof(de106));
        memset(retGetTag, 0, sizeof(retGetTag));

        // Bit 106 - Conjunto 61 - Tag 6
        msg->getSegmentData(de106, &tamDe, ElfIdMap::elf_to_id("DE106_ELO"), 0);

        if (Utility::getTagConjDadosDe106(de106, tamDe, 61, 6, retGetTag))
        {
            OTraceDebug("formatExpiracaoToken - Encontrado a Tag 6 do conjunto de dados 61 do DE 61 da Elo\n");
            compExpYYMM(retGetTag, tokenExp);
        }
    }
    else if (Utility::isVisa(msg))
    {
        char retSeg[16] = {0};
        int tamSeg = 0;

        memset(retSeg, 0, sizeof(retSeg));

        // Visa Field 123 Dataset 0x68 TAG 0x06
        msg->getSegmentData(retSeg, &tamSeg, ElfIdMap::elf_to_id(IST_SEG_NETWORK_SETTLEMENT_DATA), TOKEN_EXPIRY_DATE);
        if (tamSeg > 0)
        {
            OTraceDebug("formatExpiracaoToken - Encontrado a Tag 6 do dataset 68 do DE 123 da Visa\n");
            compExpYYMM(retSeg, tokenExp);
        }
    }

    if (strlen(tokenExp) > 0)
    {
        // jsonObject["expiracaoToken"] = std::string(tokenExp);
        jsonObject["expiracaoToken"] = atoi(tokenExp);
    }
}

void FmtBroker::formatSolicitanteToken(ShcmsgHeader *msg, json::object &jsonObject)
{
    char tokenReqId[16] = {0};
    if (Utility::isELO(msg))
    {
        char de106[1024] = {0};
        int tam = 0;
        char ret[128] = {0};
        msg->getSegmentData(de106, &tam, ElfIdMap::elf_to_id("DE106_ELO"), 0);
        if (Utility::getTagConjDadosDe106(de106, tam, 61, 2, ret))
        {
            std::snprintf(tokenReqId, sizeof(tokenReqId), "%.14s", ret);
        }
    }
    else if (Utility::isVisa(msg))
    {
        char retSeg[16] = {0};
        int tamSeg = 0;
        msg->getSegmentData(retSeg, &tamSeg, mSegmentIdNwSetData, TOKEN_REQUESTER_ID);
        if (tamSeg > 0)
            std::snprintf(tokenReqId, sizeof(tokenReqId), "%.14s", retSeg);
    }
    if (std::strlen(tokenReqId) > 0)
        jsonObject["solicitanteToken"] = std::string(tokenReqId);
}

void FmtBroker::formatScoreBandeira(ShcmsgHeader *msg, json::object &jsonObject)
{
    char lBuffer[512] = {0};
    int len = 0;

    memset(lBuffer, 0, sizeof(lBuffer));

    if (Utility::isVisa(msg))
    {
        msg->getSegmentData(lBuffer, &len, ElfIdMap::elf_to_id(IST_SEG_NETWORK_SETTLEMENT_DATA), VISA_ORA_REASON_CODE);
        if (len >= 2) // de62.21 veio da Visa!
        {
            char buffer2[8] = {0};
            memcpy(buffer2, lBuffer, 2);
            buffer2[2] = 0;
            jsonObject["scoreBandeira"] = atoi(buffer2);
        }
    }
    else if (Utility::isELO(msg))
    {
        // rever  ELO = DE124.6(11,13) Score de Fraude Bandeira Elo.

        msg->getSegmentData(lBuffer, &len, ElfIdMap::elf_to_id("DE124_ELO"), 0);
        if (len > 0)
        {
            char buffer2[8] = {0};
            memcpy(buffer2, lBuffer + 7 + 4, 3);
            buffer2[3] = 0;
            jsonObject["scoreBandeira"] = atoi(buffer2);
        }
    }
}

void FmtBroker::formatScoreAdquirente(ShcmsgHeader *msg, json::object &jsonObject)
{
    if (Utility::isELO(msg))
    {
        char buf[512] = {0};
        int len = 0;
        msg->getSegmentData(buf, &len, ElfIdMap::elf_to_id("DE124_ELO"), 0);
        if (len > 0)
        {
            char v[5] = {0};
            std::memcpy(v, buf + 7, 4);
            jsonObject["scoreAdquirente"] = std::atoi(v);
        }
    }
}

void FmtBroker::formatCodigoResposta(ShcmsgHeader *msg, json::object &jsonObject)
{
    // TODO a definir refinar
    // A VALIDAR COM BRADESCO
    if (strlen(msg->msg.alpha_response_code) > 0)
    {
        jsonObject["codigoResposta"] = std::string(msg->msg.alpha_response_code);
    }
}

void FmtBroker::formatCodigoRespostaExtendido(ShcmsgHeader *msg, json::object &jsonObject)
{
    // Campo opcional (a definir com o emissor/adquirente)
}

void FmtBroker::formatTipoFraude(ShcmsgHeader *msg, json::object &jsonObject)
{
    // Campo opcional / a definir
}

void FmtBroker::formatIndicadorEcommerce(ShcmsgHeader *msg, json::object &jsonObject)
{
    // TODO validar origem dos dados
    std::string indicadorEcommerce = "";

    if (Utility::isVisa(msg))
    {
        // DE060.8
        if (strlen(msg->msg.sec_fmt_code) > 0)
        {
            indicadorEcommerce = std::string(msg->msg.sec_fmt_code);

            // enviar somente se valores '05', '06', '07' ou '08'
            if (indicadorEcommerce != "05" && indicadorEcommerce != "06" && indicadorEcommerce != "07" && indicadorEcommerce != "08")
            {
                // nao enviar campo
                indicadorEcommerce = "";
            }
        }
    }
    else if (Utility::isELO(msg))
    {
        // DE062 *ECI tag
        char segBuffer[1024] = {0};
        int segLen = 0;
        msg->getSegmentData(segBuffer, &segLen, ElfIdMap::elf_to_id("ELO_062_TID_ECI"), 0);

        if (segLen > 0)
        {
            std::string l_seg_ELO_062_TID_ECI(segBuffer);
            OTraceDebug("formatIndicadorEcommerce - Segmento ELO_062_TID_ECI[%s]\n", l_seg_ELO_062_TID_ECI.c_str());
            indicadorEcommerce = UtilsSO::parseTLV(4, 3, l_seg_ELO_062_TID_ECI, "*ECI", "", "");
            OTraceDebug("formatIndicadorEcommerce - tag ECI[%s]\n", indicadorEcommerce.c_str());
        }
    }

    if (indicadorEcommerce.length() > 0)
    {
        jsonObject["indicadorEcommerce"] = indicadorEcommerce;
    }
}

void FmtBroker::formatCodigoSegFatorAut(ShcmsgHeader *msg, json::object &jsonObject)
{
    // TODO validar origem dos dados
    std::string codigoSegFatorAut("");

    if (Utility::isVisa(msg))
    {
        // DE126.9 (posicao3-4/byte2)
        struct shc_data *ptr_shc_data = NULL;
        ptr_shc_data = (struct shc_data *)msg->msg.shc_data_buffer;

        if (strlen(ptr_shc_data->trans_stain) >= 4)
        {
            codigoSegFatorAut = std::string(ptr_shc_data->trans_stain).substr(2, 2);
        }
    }
    else if (Utility::isELO(msg))
    {
        char segBuffer[1024] = {0};
        int segLen = 0;
        msg->getSegmentData(segBuffer, &segLen, ElfIdMap::elf_to_id("DE122_ELO"), 0);
        if (segLen > 0)
        {
            std::string l_seg_DE122_ELO(segBuffer);
            OTraceDebug("formatCodigoSegFatorAut - Segmento DE122_ELO[%s]\n", l_seg_DE122_ELO.c_str());

            std::string buffer3DS = l_seg_DE122_ELO.substr(0, 2);

            if (buffer3DS == "02")
            {
                // DE122 formato antigo de tags (nao TLV)
                codigoSegFatorAut = l_seg_DE122_ELO.substr(2, 2);
            }
            else if (buffer3DS == "01")
            {
                std::string retDE122Tag02 = UtilsSO::parseTLV(2, 2, l_seg_DE122_ELO.substr(2).c_str(), "02");
                if (retDE122Tag02.length() >= 4)
                {
                    // DE122 id 01(TLV) com tag 02 presente
                    codigoSegFatorAut = retDE122Tag02.substr(2, 2);
                }
            }
        }
    }

    if (codigoSegFatorAut.length() > 0)
    {
        jsonObject["codigoSegFatorAut"] = codigoSegFatorAut;
    }
}

void FmtBroker::formatAtcCartao(ShcmsgHeader *msg, json::object &jsonObject)
{
    // TODO validar origem dos dados
    std::string atcCartao("");

    // DE055 tag 9F36

    typedef struct emv_segment_buf
    {
        struct emvbuf emvp;
        char issuer_script[128 + 1];
        char issuer_script_2[128 + 1];
    } emv_segment;

    emv *emvp;
    emv_segment emv_seg2;
    emvp = (emv *)&emv_seg2.emvp;
    memset(emvp, 0, sizeof(emv));
    int l_tam_ = sizeof(emv);
    int numericAtc = 0;

    msg->getSegmentData((char *)emvp, &l_tam_, EMV_BUF_id, 0);

    OTraceDebug("==========================================================\n");
    OTraceDebug("  LEN-EMV_BUF          : [%d]\n", l_tam_);
    OTraceDebug("  LEN-app_txn_counter  : [%d]\n", strlen(emvp->app_txn_counter));
    OTraceDebug("  app_txn_counter      : [%s]\n", emvp->app_txn_counter);
    OTraceDebug("==========================================================\n");

    if (l_tam_ <= 0 || emvp->app_txn_counter[0] == '\0')
    {
        atcCartao = "";
    }
    else
    {
        numericAtc = std::strtol(emvp->app_txn_counter, 0, 16);
        atcCartao = std::to_string(numericAtc);
    }

    // mWebServiceHandler.setFieldJsonRequestNivel( "Dbtran", "atcCard", emvp->app_txn_counter );
    OTraceDebug("formatAtcCartao ATC-HEXA [%s] ATC-DECIMAL [%d]\n", emvp->app_txn_counter, numericAtc);

    if (atcCartao.length() > 0)
    {
        jsonObject["atcCartao"] = std::stoi(atcCartao);
    }
}

void FmtBroker::formatAtcIST(ShcmsgHeader *msg, json::object &jsonObject)
{
    // TODO validar origem dos dados
    std::string atcIST("");

    char segBuffer[1024] = {0};
    int segLen = 0;
    msg->getSegmentData(segBuffer, &segLen, ElfIdMap::elf_to_id("ATC_HOST"), 0);
    if (segLen > 0)
    {
        atcIST = std::string(segBuffer);
        OTraceDebug("formatAtcIST - Segmento ATC_HOST[%s]\n", atcIST.c_str());
    }

    if (atcIST.length() > 0)
    {
        jsonObject["atcIST"] = std::stoi(atcIST);
    }
}

void FmtBroker::formatIndicadorCarteiraDigital(ShcmsgHeader *msg, json::object &jsonObject)
{
    // TODO validar origem dos dados
    char segBuffer[1024] = {0};
    int segLen = 0;
    std::string indicadorCarteiraDigital("N");

    if (Utility::isVisa(msg))
    {
        // DE125.DS02.09 ???
        msg->getSegmentData(segBuffer, &segLen, ElfIdMap::elf_to_id("VISA_WALLET_ACCOUNT_ID"), 0);
        if (segLen > 0)
        {
            indicadorCarteiraDigital = "S";
            OTraceDebug("formatIndicadorCarteiraDigital - Segmento VISA_WALLET_ACCOUNT_ID[%s]\n", segBuffer);
        }
    }
    else if (Utility::isELO(msg))
    {
        // DE048.*WID ???
        msg->getSegmentData(segBuffer, &segLen, ElfIdMap::elf_to_id("DE048_ELO"), 0);
        if (segLen > 0)
        {
            std::string lDE048Data(segBuffer);
            std::string lTagDataWID = UtilsSO::parseTLV(4, 3, lDE048Data, "*WID", "", "");

            if (lTagDataWID.length() > 0)
            {
                indicadorCarteiraDigital = "S";
                OTraceDebug("formatIndicadorCarteiraDigital - Segmento DE048_ELO.WID[%s]\n", lTagDataWID.c_str());
            }
        }
    }

    if (indicadorCarteiraDigital.length() > 0)
    {
        jsonObject["indicadorCarteiraDigital"] = indicadorCarteiraDigital;
    }
}

// ===== ISO8583 mascarado =====
void FmtBroker::formatIso8583(ShcmsgHeader *msg, json::object &root)
{
    char isoRaw[AUX_MAX_BUFF_LEN] = {0};
    int isoLen = 0;

    msg->getSegmentData(isoRaw, &isoLen, ElfIdMap::elf_to_id("ISO_RAW_MSG"), 0);
    if (isoLen <= 0)
    {
        // Sem ISO bruto no segmento
        return;
    }

    Iso8583ParserBase *parser = nullptr;
    if (Utility::isVisa(msg))
        parser = &parseVisa;
    else if (Utility::isELO(msg))
        parser = &parseElo;
    if (!parser)
        return;

    if (parser->parseIsoRaw(reinterpret_cast<unsigned char *>(isoRaw), isoLen))
    {
        json::array arr;

        // getFields(): map<string,string> com {elementId, fieldData}
        auto fields = parser->getFields();
        for (const auto &kv : fields)
        {
            std::string elementId = kv.first;
            std::string fieldData = kv.second;
            std::string fieldMask = fieldData;

            // Não exportar bitmap como campo
            if (elementId == "001")
                continue;

            // Mascaramento (PAN, trilhas, EMV/PIN)
            if (elementId == "002")
            {
                fieldMask = shc_maskpan(fieldMask.c_str());
            }
            else if (elementId == "035" || elementId == "045")
            {
                char maskedTrack[128] = {0};
                Utility::MaskTrk2(const_cast<char *>(fieldMask.c_str()), maskedTrack);
                fieldMask = maskedTrack;
            }
            else if (elementId == "014" || elementId == "052" || elementId == "055")
            {
                if (fieldMask.size() > 4 && fieldMask.size() < 20)
                {
                    std::fill(fieldMask.begin() + 4, fieldMask.end(), '*');
                }
                else if (fieldMask.size() >= 20)
                {
                    fieldMask = "**********";
                }
                else
                {
                    std::fill(fieldMask.begin(), fieldMask.end(), '*');
                }
            }

            json::object obj;
            obj["elementId"] = elementId;
            obj["fieldData"] = fieldMask;
            arr.push_back(obj);
        }
        root["iso8583"] = arr;
    }
}

// ===== Utilidades herdadas =====
std::string FmtBroker::getPanFromTrack(char *trackData)
{
    // Idêntico à referência: extrai os dígitos do PAN a partir da trilha 2
    if (!trackData)
        return {};
    char *ptrTrack = trackData;
    while (*ptrTrack && !std::isdigit(static_cast<unsigned char>(*ptrTrack)))
        ++ptrTrack;

    char out[SHC_TRACK2_LEN + 1] = {0};
    char *w = out;
    while (*ptrTrack && std::isdigit(static_cast<unsigned char>(*ptrTrack)))
    {
        *w++ = *ptrTrack++;
    }
    *w = '\0';
    return std::string(out);
}

void FmtBroker::compExpYYMM(std::string expiry, char *buffer)
{
    int mdays[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (expiry.size() < 4 || !buffer)
        return;
    int y = std::atoi(expiry.substr(0, 2).c_str());
    int m = std::atoi(expiry.substr(2, 2).c_str());
    if (m <= 0 || m > 12)
        return;

    int year = y + 2000;
    if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)
        mdays[2] = 29;
    int day = mdays[m];
    std::snprintf(buffer, 16, "%04d%02d%02d", year, m, day);
}

int FmtBroker::getEntryCode(ShcmsgHeader *msg)
{
    char buffer[32] = {0};
    int len = 0;

    if (Utility::isELO(msg))
    {
        return msg->msg.pos_entry_code;
    }
    else
    {
        if (shcIsResponse(msg))
        {
            msg->getSegmentData(buffer, &len, NETWORK_SETTLEMENT_DATA_RESP_id, POS_ENTRY_CODE);
        }
        else
        {
            msg->getSegmentData(buffer, &len, NETWORK_SETTLEMENT_DATA_id, POS_ENTRY_CODE);
        }
    }

    strtrim(buffer, buffer);
    std::string entryCode(buffer);

    // OTraceDebug( "Call getEntryCode( )[%s]\n", entryCode.c_str( ) );

    return std::atoi(entryCode.c_str());
}

bool FmtBroker::isFromECommerce(ShcmsgHeader *msg)
{
    int de22 = getEntryCode(msg) / 10;
    if (Utility::isELO(msg))
    {
        return (de22 == 7 || de22 == 82);
    }
    if (Utility::isVisa(msg))
    {
        short posEntryCode = getEntryCode(msg);
        return (((posEntryCode & SH_POS_PINPANENTRY_MASK) / 10) == 10) ||
               (msg->msg.pos_condition_code == SH_POS_COND_REQ_MAIL) ||
               (msg->msg.pos_condition_code == SH_POS_COND_REQ_ELECT);
    }
    return false;
}

bool FmtBroker::isChargeback(ShcmsgHeader *msg)
{
    return (msg->msg.origmsg == 400 || msg->msg.origmsg == 420);
}

bool FmtBroker::tokenIsValid()
{
    OTraceDebug("FmtBroker::tokenIsValid\n");

    if (!tokenEnable())
    {
        return true; // Se token não é obrigatório, sempre válido
    }

    if (mValidTokenRequest.empty())
    {
        OTraceDebug("Token vazio, considerado inválido\n");
        return false;
    }

    // Validação simples de tempo (se configurado)
    if (mSecondsToExpire > 0)
    {
        time_t now = time(NULL);
        if (now >= mSecondsToExpire)
        {
            OTraceDebug("Token expirado\n");
            return false;
        }
    }

    return true;
}

void FmtBroker::getNewToken()
{
    OTraceDebug("FmtBroker::getNewToken\n");

    // Para Kafka, token pode ser usado para autenticação SASL
    // ou ser um token customizado do seu domínio

    if (!tokenEnable())
    {
        return;
    }

    // Por enquanto, implementação básica
    if (!mTokenClientId.empty() && !mTokenClientSecret.empty())
    {
        // Simula obtenção de token
        mValidTokenRequest = "Bearer " + mTokenClientId; // Implementação básica

        // Calcula expiração (ex: 1 hora)
        time_t now = time(NULL);
        mSecondsToExpire = now + 3600; // 1 hora

        OTraceDebug("Novo token obtido (simulado)\n");
    }
}

void FmtBroker::buildExceptionErrorResponse(ShcmsgHeader *msg)
{
    OTraceDebug("FmtBroker::buildExceptionErrorResponse\n");

    // Para Kafka, não há resposta direta como HTTP
    // Mas podemos logar o erro

    msg->msg.respcode = 96; // Erro do sistema

    OTraceError("FmtBroker: Erro de exceção na transação trace=%06d\n",
                msg->msg.trace);
}

void FmtBroker::buildErrorResponse(ShcmsgHeader *msg)
{
    OTraceDebug("FmtBroker::buildErrorResponse\n");

    // INCREMENTA MSGTYPE
    if (shcIsRequest(msg))
    {
        msg->msg.msgtype += 10;
    }

    // imf.respcode / imf.alpha_response_code
    msg->msg.respcode = 96;
    sprintf(msg->msg.alpha_response_code, "96");

    // imf.addresponse
    memset(msg->msg.addresponse, 0, sizeof(msg->msg.addresponse));
    snprintf(msg->msg.addresponse, sizeof(msg->msg.addresponse), "System Error");

    msg->msg.respcode = convertResponseCode(400, msg);

    OTraceDebug("  msgtype..............[%d]\n", msg->msg.msgtype);
    OTraceDebug("  origmsg..............[%d]\n", msg->msg.origmsg);
    OTraceDebug("  respcode.............[%d]\n", msg->msg.respcode);
    OTraceDebug("  alpha_response_code..[%s]\n", msg->msg.alpha_response_code);
    OTraceDebug("  addresponse..........[%s]\n", msg->msg.addresponse);
}

int FmtBroker::convertResponseCode(int httpStatusCode, ShcmsgHeader *msg)
{
    int returnResponseCode = 96;
    int responseCode = msg->msg.respcode;
    int resultConvertRespCode = -1;

    std::string alphaResponseCode(msg->msg.alpha_response_code);
    std::string errorMessage(msg->msg.addresponse);

    char srcRespcode[5] = {0};
    char destRespcode[5] = {0};
    char searchMessageType[5] = {0};
    char txnDest[31] = "\0";

    OTraceDebug("FmtBroker::convertResponseCode\n");

    // 4xx ou 5xx
    if ((httpStatusCode / 100 == 4) || (httpStatusCode / 100 == 5))
    {
        memset(srcRespcode, 0, sizeof(srcRespcode));
        memset(destRespcode, 0, sizeof(destRespcode));
        memset(searchMessageType, 0, sizeof(searchMessageType));
        memset(txnDest, 0, sizeof(txnDest));

        snprintf(srcRespcode, sizeof(srcRespcode), "%03d", responseCode);
        snprintf(txnDest, sizeof(txnDest), "%s", "FmtBroker");
        snprintf(searchMessageType, sizeof(searchMessageType), "%04d", msg->msg.msgtype);
        // snprintf(searchMessageType, sizeof(searchMessageType), "%04d", msg->msg.origmsg);
    }

    if (resultConvertRespCode >= 0)
    {
        returnResponseCode = atoi(destRespcode);
        alphaResponseCode = destRespcode;

        switch (returnResponseCode)
        {
        case 13:
            errorMessage = "Message Format Error";
            break;

        case 14:
            errorMessage = "Invalid Card";
            break;

        case 30:
            errorMessage = "Message Format Error";
            break;

        case 51:
            errorMessage = "Invalid Amount";
            break;

        case 62:
            errorMessage = "Restricted Card";
            break;

        case 76:
            errorMessage = "Invalid Account";
            break;

        case 93:
            errorMessage = "Invalid Acquirer";
            break;

        default:
            errorMessage = "System Error";
            break;
        }
    }
    else
    {
        returnResponseCode = responseCode;
    }

    snprintf(msg->msg.alpha_response_code, sizeof(msg->msg.alpha_response_code), alphaResponseCode.c_str());
    snprintf(msg->msg.addresponse, sizeof(msg->msg.addresponse), errorMessage.c_str());

    OTraceDebug("  httpStatusCode........[%d]\n", httpStatusCode);
    OTraceDebug("  responseCode..........[%d]\n", responseCode);
    OTraceDebug("  alpha_response_code...[%s]\n", msg->msg.alpha_response_code);
    OTraceDebug("  addresponse...........[%s]\n", msg->msg.addresponse);
    OTraceDebug("  returnResponseCode....[%d]\n", returnResponseCode);
    OTraceDebug("----------------------------\n");

    return returnResponseCode;
}

void FmtBroker::logInfoData(std::string comment, ShcmsgHeader *msg)
{
    mb_init();

    shc_initsys();

    dbm_init();
    if ((ist_otrace_get_level() >= OTRACE_INFO) && (msg != NULL))
    {
        // obtem dados dos segmentos, quando necessario

        // refnum
        char lRefnumFis[16] = {0};
        int lRefnumDataLen = 0;
        memset(lRefnumFis, 0, sizeof(lRefnumFis));
        msg->getSegmentData(lRefnumFis, &lRefnumDataLen, ElfIdMap::elf_to_id(REF_NUM_FIS), 0);

        if (lRefnumDataLen == 0)
        {
            memset(lRefnumFis, 0, sizeof(lRefnumFis));
        }

        // pan
        std::string maskPanRemoveNoLeftZero = "";
        if (msg->msg.pan && (strlen(msg->msg.pan) > 10))
        {
            // obtem dados do campo pan
            maskPanRemoveNoLeftZero = std::string(msg->msg.pan);
        }
        else if (msg->msg.track2 && (strlen(msg->msg.track2) > 10))
        {
            // obtem dados do campo trilha2
            maskPanRemoveNoLeftZero = getPanFromTrack(msg->msg.track2);
        }

        // remove zeros a esquerda
        maskPanRemoveNoLeftZero.erase(0, maskPanRemoveNoLeftZero.find_first_not_of('0'));

        // obtem pan mascarado
        std::string maskPan = std::string(shc_maskpan(maskPanRemoveNoLeftZero.c_str()));

        // loga campos chaves da transacao
        OTraceInfo("%s: m%04d/p%06d/t%06d/"
                   "term%s/loc%s/refnum%s/fisrefnum%s/"
                   "ldate%08d/ltime%06d/tdate%08d/ttime%06d/"
                   "pan%s/amt%.2lf/rc%02d\n",
                   comment.c_str(),
                   // msg->msg.msgtype,
                   msg->msg.origmsg,
                   msg->msg.pcode,
                   msg->msg.trace,
                   msg->msg.termid,
                   msg->msg.termloc,
                   msg->msg.refnum,
                   lRefnumFis,
                   msg->msg.local_date,
                   msg->msg.local_time,
                   msg->msg.trandate,
                   msg->msg.trantime,
                   maskPan.c_str(),
                   dbm_dectodbl(&msg->msg.amount),
                   msg->msg.respcode);
    }
}

// ===== Métodos de compatibilidade HTTP (sem uso prático no Kafka) =====
std::string FmtBroker::getHostServiceAddress()
{
    // Retorna bootstrap servers para compatibilidade
    return mBootstrapServers;
}

std::string FmtBroker::getRequestUri()
{
    // Retorna tópico para compatibilidade
    return mTopic;
}
