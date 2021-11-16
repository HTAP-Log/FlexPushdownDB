//
// Created by Elena Milkai on 10/14/21.
//
#include <deltamanager/CacheHandler.h>

using namespace normal::htap::deltamanager;

CacheHandler::CacheHandler(const std::string& OperatorName,
                     const std::string& tableName,
                     const std::shared_ptr<Partition> partition,
                     const long queryId):
                     core::Operator(OperatorName, "CacheHandler", queryId){
    tableName_ = tableName;
    partition_ = partition;
}

std::shared_ptr<CacheHandler> CacheHandler::make(const std::string& OperatorName,
                                                   const std::string& tableName,
                                                   const std::shared_ptr<Partition> partition,
                                                   const long queryId) {
    return std::make_shared<CacheHandler>(OperatorName, tableName, partition, queryId);
}

void CacheHandler::onReceive(const core::message::Envelope &msg) {
    if (msg.message().type() == "StartMessage") {
        this->onStart();
    } else if (msg.message().type() == "LoadDeltasRequestMessage") {  //DeltaMerge sends a request to CacheHandler
        auto loadDeltasMessage = dynamic_cast<const LoadDeltasRequestMessage &>(msg.message());
        this->OnDeltasRequest(loadDeltasMessage);
    } else if (msg.message().type() == "StoreTailRequestMessage") {  //Request for periodic tail reading arrived
        auto tailMessage = dynamic_cast<const StoreTailRequestMessage &>(msg.message());
        this->OnTailRequest(tailMessage);
    } else if (msg.message().type() == "TupleMessage"){ //DeltaCacheActor sends response to CacheHandler
        auto repsonseMessage = dynamic_cast<const TupleMessage &>(msg.message());
        this->OnReceiveResponse(repsonseMessage);
    } else {
            throw std::runtime_error(fmt::format("Unrecognized message type: {}, {}",
                                                 msg.message().type(),
                                                 name()));
        }
}


void CacheHandler::onStart() {
    SPDLOG_INFO("Starting operator '{}'", name());
}

void CacheHandler::OnDeltasRequest(const LoadDeltasRequestMessage &message){

    const auto &deltaKey = message.getDeltaKey();
    const auto &sender = name();
    SPDLOG_CRITICAL("Message of type LoadDeltasRequestMessage was received from {} to {}.", message.sender(), name());
    ctx()->send(LoadDeltasRequestMessage::make(deltaKey, sender), "DeltaCache")
            .map_error([](auto err) { throw std::runtime_error(err); });
    SPDLOG_CRITICAL("Message of type LoadDeltasRequestMessage was send from {} to DeltaCacheActor.", name());

}


void CacheHandler::OnTailRequest(const StoreTailRequestMessage &message){
    const auto &tailKey = message.getTailKey();
    const auto &sender = message.sender();
    ctx()->send(StoreTailRequestMessage::make(tailKey, sender), "DeltaCache")
            .map_error([](auto err) {throw std::runtime_error(err); });
}


void CacheHandler::OnReceiveResponse(const TupleMessage &message){
    SPDLOG_CRITICAL("Message of type {} arrived to {} from DeltaCacheActor.", message.type(), name());
    const auto &deltaTuples = message.tuples();
    const auto &sender = name();
    std::shared_ptr<TupleMessage>
            response = std::make_shared<TupleMessage>(deltaTuples, sender);
    ctx()->send(response, "DeltaMerge-lineorder-0")
            .map_error([](auto err) { throw std::runtime_error(err); });
    ctx()->notifyComplete();
}
