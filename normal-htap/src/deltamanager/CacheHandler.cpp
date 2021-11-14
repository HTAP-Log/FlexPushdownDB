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
    } else if (msg.message().type() == "LoadDeltasResponseMessage"){ //DeltaCacheActor sends response to CacheHandler
        // probably not used since the DeltaCacheActor is directly passing the <TupleSet2> message to the consumer of
        // CacheHandler
    } else {
            throw std::runtime_error(fmt::format("Unrecognized message type: {}, {}",
                                                 msg.message().type(),
                                                 name()));
        }
}


void CacheHandler::onStart() {
    SPDLOG_DEBUG("Starting operator '{}'", name());
}

void CacheHandler::OnDeltasRequest(const LoadDeltasRequestMessage &message){

    const auto &deltaKey = message.getDeltaKey();
    const auto &sender = message.sender();

    ctx()->send(LoadDeltasRequestMessage::make(deltaKey, sender), "SegmentCache")
            .map_error([](auto err) { throw std::runtime_error(err); });
}


void CacheHandler::OnTailRequest(const StoreTailRequestMessage &message){
    const auto &tailKey = message.getTailKey();
    const auto &sender = message.sender();
    ctx()->send(StoreTailRequestMessage::make(tailKey, sender), "SegmentCache")
            .map_error([](auto err) {throw std::runtime_error(err); });
}

