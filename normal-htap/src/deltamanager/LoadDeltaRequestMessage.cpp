//
// Created by Elena Milkai on 10/25/21.
//

#include <deltamanager/LoadDeltaRequestMessage.h>

using namespace normal::htap::deltamanager;

LoadDeltaRequestMessage::LoadDeltaRequestMessage(std::vector<std::shared_ptr<DeltaCacheKey>> deltaKeys,
const std::string &sender):
        Message("LoadDeltaRequestMessage", sender),
        deltaKeys_(std::move(deltaKeys)) {}

std::shared_ptr<LoadDeltaRequestMessage> LoadDeltaRequestMessage::make(
        const std::vector<std::shared_ptr<DeltaCacheKey>>& deltaKeys,
        const std::string &sender){
    return std::make_shared<LoadDeltaRequestMessage>(deltaKeys, sender);
}

[[nodiscard]] const std::vector<std::shared_ptr<DeltaCacheKey>> &LoadDeltaRequestMessage::getDeltaKeys() const {
    return deltaKeys_;
}

[[nodiscard]] std::string LoadDeltaRequestMessage::toString() const {

    /*std::string s = "deltaKeys : [";
    for (auto it = deltaKeys_.begin(); it != deltaKeys_.end(); ++it) {
        s += fmt::format("{}", it->get()->toString());
        if (std::next(it) != deltaKeys_.end())
            s += ",";
    }
    s += "]";

    return s;*/
}