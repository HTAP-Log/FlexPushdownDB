//
// Created by Elena Milkai on 11/8/21.
//

#include <deltamanager/LoadDeltasRequestMessage.h>

using namespace normal::htap::deltamanager;

LoadDeltasRequestMessage::LoadDeltasRequestMessage(std::shared_ptr<DeltaCacheKey> deltaKey,
                                                   const std::string &sender):
        Message("LoadDeltasRequestMessage", sender),
        deltaKey_(std::move(deltaKey)) {}

std::shared_ptr<LoadDeltasRequestMessage> LoadDeltasRequestMessage::make(
        const std::shared_ptr<DeltaCacheKey>& deltaKey,
        const std::string &sender){
    return std::make_shared<LoadDeltasRequestMessage>(deltaKey, sender);
}

[[nodiscard]] const std::shared_ptr<DeltaCacheKey> &LoadDeltasRequestMessage::getDeltaKey() const {
    return deltaKey_;
}

[[nodiscard]] std::string LoadDeltasRequestMessage::toString() const {

    std::string s = "deltaKey : [";
    s += fmt::format("{}", deltaKey_->getTableName());
    s += ",";
    s += fmt::format("{}", deltaKey_->getPartition());
    s += "]";
    return s;
}