//
// Created by Elena Milkai on 10/14/21.
//

#include <deltamanager/CacheHandler.h>

using namespace normal::htap::deltamanager;

CacheHandler::CacheHandler(const std::string& OperatorName,
                     const std::string& tableName,
                     const int &partition,
                     const int &timestamp,
                     const long queryId):
                     core::Operator(OperatorName, "CacheHandler", queryId){
    tableName_ = tableName;
    partition_ = partition;
    timestamp_ = timestamp;
}

std::shared_ptr<CacheHandler> CacheHandler::make(const std::string& OperatorName,
                                                   const std::string& tableName,
                                                   const int &partition,
                                                   const int &timestamp,
                                                   const long queryId) {
    return std::make_shared<CacheHandler>(OperatorName, tableName, partition, timestamp, queryId);
}

void CacheHandler::onReceive(const core::message::Envelope &msg) {
    if (msg.message().type() == "StartMessage") {
        this->onStart();
    } else if (msg.message().type() == "LoadDeltas") {  // DeltaMerge sends a request of m-deltas to the CacheHandler
        auto loadDeltasMessage = dynamic_cast<const LoadDeltasRequestMessage &>(msg.message());
        this->OnLoadDeltas(loadDeltasMessage);
    } else if (msg.message().type() == "TailResponse") {  // GetTailDelta sends the response with the tail
        auto tailMessage = dynamic_cast<const LoadTailResponseMessage &>(msg.message());
        this->OnTailResponse(tailMessage);
    } else {
        throw std::runtime_error(fmt::format("Unrecognized message type: {}, {}",
                                             msg.message().type(),
                                             name()));
    }
}

void CacheHandler::onStart() {
    SPDLOG_DEBUG("Starting operator '{}'", name());
}

void CacheHandler::OnLoadDeltas(const LoadDeltasRequestMessage &message){

    const auto &deltaKeys = message.getDeltaKeys();
    for(unsigned int i=0; i<deltaKeys.size(); i++){
        if(deltaKeys[i]->getTimestamp())
    }
    std::shared_ptr<LoadTailRequestMessage>
           tailRequest = std::make_str<normal::htap::deltamanager::LoadTailRequestMessage>(, this->name());
    this->sendResponse(message.getDeltaKeys());  // send response with the deltas to DeltaMerge

}


void CacheHandler::OnTailResponse(const LoadTailResponseMessage &message){

   //this->sendResponse();

}


void CacheHandler::sendResponse(const std::vector <std::shared_ptr<DeltaCacheKey>> &deltaKeys){

    std::shared_ptr<normal::core::message::Message>
            message = std::make_shared<core::message::TupleMessage>(deltaPartitionTable->toTupleSetV1(), this->name());

    ctx()->tell(message);
    ctx()->notifyComplete();
    ctx()->send();

}