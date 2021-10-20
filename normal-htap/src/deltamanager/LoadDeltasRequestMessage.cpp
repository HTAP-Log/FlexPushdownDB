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

    /*std::string s = "deltaKey : [";
    for (auto it = deltaKey_.begin(); it != deltaKey_.end(); ++it) {
    for (auto it = deltaKey_.begin(); it != deltaKey_.end(); ++it) {
        s += fmt::format("{}", it->get()->toString());
        if (std::next(it) != deltaKey_.end())
            s += ",";
    }
    s += "]";

    return s;*/
}