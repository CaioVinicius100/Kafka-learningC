#ifndef __FMTBROKER_H
#define __FMTBROKER_H

// STD
#include <string>
#include <memory>

// IST core (define struct ShcmsgHeader)
#include <shc.h>

// Kafka (cppkafka)
#include <cppkafka/cppkafka.h>

// JSON
#include <boost/json.hpp>

// LOCAL
#include "segmentos.h"
#include "Iso8583ParserVisa.hpp"
#include "Iso8583ParserElo.hpp"
#include "Iso8583ParserBase.hpp"
#include "tabelas_sw_cartao.h"
#include "MailboxBaseBR.hpp"
#include "FmtBrokerProducer.hpp"

// Alias simples para JSON (usamos json::object)
namespace json = boost::json;

#define VEND_DBT               "VEND_DBT"               // 01 Compra Debito
#define ADV_VEND_DBT           "ADV_VEND_DBT"           // 02 Advice de Compra Debito
#define EST_VEND_DBT           "EST_VEND_DBT"           // 03 Reversao de Compra Debito
#define DESF_EST_VEND_DBT      "DESF_EST_VEND_DBT"      // 04 Advice de Reversao de Compra Debito
#define SAQUE_DBT              "SAQUE_DBT"              // 05 Saque com Cartao de Debito
#define ADV_SAQUE_DBT          "ADV_SAQUE_DBT"          // 06 Advice de Saque com Cartao de Debito
#define EST_SAQUE_DBT          "EST_SAQUE_DBT"          // 07 Reversao de Saque com Cartao de Debito
#define REEMB_COMP_DBT         "REEMB_COMP_DBT"         // 09 Retorno de Compra (Reembolso) Debito
#define ADV_REEMB_COMP_DBT     "ADV_REEMB_COMP_DBT"     // 10 Advice de Retorno de Compra (Reembolso) Debito
#define EST_REEMB_COMP_DBT     "EST_REEMB_COMP_DBT"     // 11 Reversao de Retorno de Compra (Reembolso) Debito
#define DESF_EST_REEMB_COMP_DBT "DESF_EST_REEMB_COMP_DBT" // 12 Advice de Reversao de Retorno de Compra (Reembolso) Debito
#define ACC_FDG_DBT            "ACC_FDG_DBT"            // 13 Account Funding Debito
#define ADV_ACC_FDG_DBT        "ADV_ACC_FDG_DBT"        // 14 Advice de Account Funding Debito
#define EST_ACC_FDG_DBT        "EST_ACC_FDG_DBT"        // 15 Reversao de Account Funding Debito
#define DESF_EST_ACC_FDG_DBT   "DESF_EST_ACC_FDG_DBT"   // 16 Advice de Reversao de Account Funding Debito
#define ORIG_CRD_DBT           "ORIG_CRD_DBT"           // 17 Original Credit Debito
#define ADV_ORIG_CRD_DBT       "ADV_ORIG_CRD_DBT"       // 18 Advice de Original Credit Debito
#define EST_ORIG_CRD_DBT       "EST_ORIG_CRD_DBT"       // 19 Reversao de Original Credit Debito
#define DESV_EST_ORIG_CRD_DBT  "DESV_EST_ORIG_CRD_DBT"  // 20 Advice de Reversao de Original Credit Debito
#define PREAUT_DBT             "PREAUT_DBT"             // 21 Pre-autorizacao de Compra Debito

// Removido forward declaration: agora usamos definição real de ShcmsgHeader de <shc.h>

class FmtBroker : public MailboxBaseBR
{
public:
    FmtBroker();
    ~FmtBroker();

    // Métodos chamados pelo Main
    void init();
    void exec();

protected:
    void logInfoData( std::string, ShcmsgHeader* );
    void postInit();         // Agora inicializa transporte TCP
    void postClearMsg();     // Limpa buffers TCP/estados leves

    std::string getHostServiceAddress();  // Retorna host (Kafka bootstrap)
    std::string getRequestUri();          // Retorna tópico Kafka

    bool getServiceName( ShcmsgHeader* );
    void buildJsonRequest( ShcmsgHeader*, json::object& );
    void processPreRequest( ShcmsgHeader*, json::object& );
    void processTransaction( ShcmsgHeader* );

    void buildExceptionErrorResponse( ShcmsgHeader* );
    void buildErrorResponse( ShcmsgHeader* );

private:
    bool getTransactionName( ShcmsgHeader* );
    bool indicadorReversaoParcial( ShcmsgHeader* );
    bool isVendaDebito();
    bool isReversaoVendaDebito();
    bool isAdviceVendaDebito();
    bool isVendaDebitoCashback( ShcmsgHeader* );
    bool isAccountFunding();
    bool isChargeback( ShcmsgHeader* );
    int  convertResponseCode( int, ShcmsgHeader* );
    std::string getPanFromTrack( char* );
    void compExpYMM( std::string expiry, char* buffer );
    int  getEntryCode( ShcmsgHeader* );
    bool isFromECommerce( ShcmsgHeader* );
    bool tokenEnable();
    bool tokenIsValid();
    void getNewToken();

    void formatCodigoIdTransacao( ShcmsgHeader*, json::object& );
    void formatCanal( ShcmsgHeader*, json::object& );
    void formatAplicativoOrigem( ShcmsgHeader*, json::object& );
    void formatIdentificacao( ShcmsgHeader*, json::object& );
    void formatGeolocalizacao( ShcmsgHeader*, json::object& );
    void formatTipo( ShcmsgHeader*, json::object& );
    void formatInstalacao( ShcmsgHeader*, json::object& );
    void formatTipoCliente( ShcmsgHeader*, json::object& );
    void formatTipoDocumento( ShcmsgHeader*, json::object& );
    void formatNumero( ShcmsgHeader*, json::object& );
    void formatSegmento( ShcmsgHeader*, json::object& );
    void formatTelefone( ShcmsgHeader*, json::object& );
    void formatAgencia( ShcmsgHeader*, json::object& );
    void formatConta( ShcmsgHeader*, json::object& );
    void formatDigito( ShcmsgHeader*, json::object& );
    void formatTitularidade( ShcmsgHeader*, json::object& );
    void formatNumeroCartao( ShcmsgHeader*, json::object& );
    void formatDataValidadeCartao( ShcmsgHeader*, json::object& );
    void formatTipoCartao( ShcmsgHeader*, json::object& );
    void formatProduto( ShcmsgHeader*, json::object& );
    void formatSegmentoProduto( ShcmsgHeader*, json::object& );
    void formatModoEntradaOperacao( ShcmsgHeader*, json::object& );
    void formatIndicadorSenha( ShcmsgHeader*, json::object& );
    void formatIndicadorCvv2( ShcmsgHeader*, json::object& );
    void formatTipoDebitoCredito( ShcmsgHeader*, json::object& );
    void formatIndicadorParcelado( ShcmsgHeader*, json::object& );
    void formatCodigoEstabelecimento( ShcmsgHeader*, json::object& );
    void formatCodigoPostal( ShcmsgHeader*, json::object& );
    void formatCidadeEstabelecimento( ShcmsgHeader*, json::object& );
    void formatPais( ShcmsgHeader*, json::object& );
    void formatIsoPais( ShcmsgHeader*, json::object& );
    void formatSetorAtividade( ShcmsgHeader*, json::object& );

    void formatDataHora( ShcmsgHeader*, json::object& );
    void formatDataHoraOperacional( ShcmsgHeader*, json::object& );
    void formatTipoTransacao( ShcmsgHeader*, json::object& );
    void formatEtapaTransacao( ShcmsgHeader*, json::object& );
    void formatCodigoAdquirente( ShcmsgHeader*, json::object& );
    void formatMoedaTransacao( ShcmsgHeader*, json::object& );
    void formatValorTransacao( ShcmsgHeader*, json::object& );
    void formatCodigoProcessamento( ShcmsgHeader*, json::object& );
    void formatTipoMensagem( ShcmsgHeader*, json::object& );
    void formatTipoOperacao( ShcmsgHeader*, json::object& );
    void formatIndicadorTokenizado( ShcmsgHeader*, json::object& );
    void formatExpiracaoToken( ShcmsgHeader*, json::object& );
    void formatSolicitanteToken( ShcmsgHeader*, json::object& );
    void formatScoreAdquirente( ShcmsgHeader*, json::object& );
    void formatCodigoResposta( ShcmsgHeader*, json::object& );
    void formatCodigoRespostaExtendido( ShcmsgHeader*, json::object& );
    void formatTipoFraude( ShcmsgHeader*, json::object& );
    void formatIndicadorEcommerce( ShcmsgHeader*, json::object& );
    void formatCodigoSegFatorAut( ShcmsgHeader*, json::object& );
    void formatAtcCartao( ShcmsgHeader*, json::object& );
    void formatAtcIST( ShcmsgHeader*, json::object& );
    void formatIndicadorCarteiraDigital( ShcmsgHeader*, json::object& );
    void formatIso8583( ShcmsgHeader*, json::object& );

    // Métodos específicos para Kafka
    void sendToKafka(ShcmsgHeader *msg, json::object &jsonObject);
    std::string getKafkaBootstrapServers();
    std::string getKafkaTopic();

private:
    std::string mFmtName;
    std::string mTransactionName;
    std::string mServiceContentType;
    std::string mTokenContentType;
    std::string mServiceName;
    std::string mBootstrapServers;   // bootstrap.servers do Kafka
    std::string mTopic;              // tópico Kafka
    std::string mMessageKeyField;    // campo configurado para chave da mensagem Kafka

    bool        mTokenEnable;
    std::string mTokenExpiryTime;
    std::string mTokenHostService;
    std::string mTokenUrl;
    std::string mTokenClientId;
    std::string mValidTokenRequest;
    int         mSecondsToExpire;

    static unsigned short mSegmentIdNwSetData; // NETWORK_SETTLEMENT_DATA

    // Parser ISO 8583
    bool               mSendIso8583;
    Iso8583ParserVisa  parseVisa;
    Iso8583ParserElo   parseElo;
    SW_CARTAO          m_sw_cartao;

    // Kafka Producer
    cppkafka::Configuration                  m_kafkaConfig;      // Configuração inline carregada do cfg
    std::unique_ptr<cppkafka::Producer>      m_producer;         // Caminho legado direto
    std::unique_ptr<FmtBrokerProducer>       m_simpleProducer;   // Wrapper preferencial
    bool                                     m_simpleProducerInit = false; // True se wrapper inicializado em postInit
};

#endif
