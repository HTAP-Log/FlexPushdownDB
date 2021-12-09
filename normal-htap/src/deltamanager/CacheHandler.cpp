//
// Created by Elena Milkai on 10/14/21.
//
#include <deltamanager/CacheHandler.h>

using namespace normal::htap::deltamanager;

CacheHandler::CacheHandler(const std::string& OperatorName,
                     const std::string& tableName,
                     const long partition,
                     const long queryId):
                     core::Operator(OperatorName, "CacheHandler", queryId){
    tableName_ = tableName;
    partition_ = partition;
}

std::shared_ptr<CacheHandler> CacheHandler::make(const std::string& OperatorName,
                                                   const std::string& tableName,
                                                   const long partition,
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
    } else if (msg.message().type() == "LoadDeltasResponseMessage"){ //DeltaCacheActor sends response to CacheHandler
        auto responseMessage = dynamic_cast<const LoadDeltasResponseMessage &>(msg.message());
        this->OnReceiveResponse(responseMessage);
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
    /*SPDLOG_CRITICAL("[2]. {}: Message of type {} received from {}.",
                    name(), message.type(),message.sender());*/
    ctx()->send(LoadDeltasRequestMessage::make(deltaKey, sender), "DeltaCache")
            .map_error([](auto err) { throw std::runtime_error(err); });
    //SPDLOG_CRITICAL("[3]. {}: Message of type {} was send to DeltaCacheActor.", name(), message.type());

}

void CacheHandler::OnTailRequest(const StoreTailRequestMessage &message){
    const auto &tailKey = message.getTailKey();
    const auto &sender = message.sender();
    ctx()->send(StoreTailRequestMessage::make(tailKey, sender), "DeltaCache")
            .map_error([](auto err) {throw std::runtime_error(err); });
}

void CacheHandler::OnReceiveResponse(const LoadDeltasResponseMessage &message){
    //SPDLOG_CRITICAL("[6]. {}: Message of type {} from {}.", name(), message.type(), message.sender());
    std::vector<std::shared_ptr<TupleSet2>> deltas = message.getDeltas();
    std::vector<int> timestamps = message.getTimestamps();
    const auto &sender = name();
    std::shared_ptr<LoadDeltasResponseMessage>
            response = std::make_shared<LoadDeltasResponseMessage>(deltas, timestamps, sender);
    std::string deltaCacheName = "DeltaMerge-"+this->tableName_+"-"+std::to_string(this->partition_);
    //SPDLOG_CRITICAL("~~~~~~~~~~~~~~~~~~~~~~DeltaCache operator: {}", deltaCacheName);
    ctx()->send(response, deltaCacheName)
            .map_error([](auto err) { throw std::runtime_error(err); });
    ctx()->notifyComplete();
}