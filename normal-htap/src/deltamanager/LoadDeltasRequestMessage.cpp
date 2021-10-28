//
// Created by Elena Milkai on 10/28/21.
//

#include <deltamanager/LoadDeltasRequestMessage.h>

using namespace normal::htap::deltamanager;

LoadDeltasRequestMessage::LoadDeltasRequestMessage(std::vector<std::shared_ptr<DeltaCacheKey>> deltaKeys,
                                                 const std::string &sender):
        Message("LoadDeltasRequestMessage", sender),
        deltaKeys_(std::move(deltaKeys)) {}

std::shared_ptr<LoadDeltasRequestMessage> LoadDeltasRequestMessage::make(
        const std::vector<std::shared_ptr<DeltaCacheKey>>& deltaKeys,
        const std::string &sender){
    return std::make_shared<LoadDeltasRequestMessage>(deltaKeys, sender);
}

[[nodiscard]] const std::vector<std::shared_ptr<DeltaCacheKey>> &LoadDeltasRequestMessage::getDeltaKeys() const {
    return deltaKeys_;
}

[[nodiscard]] std::string LoadDeltasRequestMessage::toString() const {

    /*std::string s = "deltaKeys : [";
    for (auto it = deltaKeys_.begin(); it != deltaKeys_.end(); ++it) {
    for (auto it = deltaKeys_.begin(); it != deltaKeys_.end(); ++it) {
        s += fmt::format("{}", it->get()->toString());
        if (std::next(it) != deltaKeys_.end())
            s += ",";
    }
    s += "]";

    return s;*/
}