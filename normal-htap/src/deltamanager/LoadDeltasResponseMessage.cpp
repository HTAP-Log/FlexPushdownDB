//
// Created by Elena Milkai on 10/25/21.
//

#include <deltamanager/LoadDeltasResponseMessage.h>

using namespace normal::htap::deltamanager;

LoadDeltasResponseMessage::LoadDeltasResponseMessage(std::unordered_map<std::shared_ptr<DeltaCacheKey>,
                                                                      std::shared_ptr<DeltaCacheData>,
                                                                      DeltaKeyPointerHash,
                                                                      DeltaKeyPointerPredicate> deltas,
                                                                      const std::string &sender,
                           std::vector<std::shared_ptr<DeltaCacheKey>> deltaKeysToCache):
                           Message("LoadDeltasResponseMessage", sender),
                           deltas_(std::move(deltas)),
                           deltaKeysToCache_(std::move(deltaKeysToCache)){}

std::shared_ptr<LoadDeltasResponseMessage> LoadDeltasResponseMessage::make(std::unordered_map<std::shared_ptr<DeltaCacheKey>,
                                                                         std::shared_ptr<DeltaCacheData>,
                                                                         DeltaKeyPointerHash,
                                                                         DeltaKeyPointerPredicate> deltas,
                                                                         const std::string &sender,
                std::vector<std::shared_ptr<DeltaCacheKey>> deltaKeysToCache){
    return std::make_shared<LoadDeltasResponseMessage>(std::move(deltas), sender, std::move(deltaKeysToCache));
}

[[maybe_unused]] [[nodiscard]] const std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>,
        DeltaKeyPointerHash, DeltaKeyPointerPredicate> &LoadDeltasResponseMessage::getDeltas() const {
    return deltas_;
}

[[nodiscard]] const std::vector<std::shared_ptr<DeltaCacheKey>> &LoadDeltasResponseMessage::getDeltaKeysToCache() const {
    return deltaKeysToCache_;
}

[[nodiscard]] std::string LoadDeltasResponseMessage::toString() const {
    /*std::string s = "Deltas loaded: {";
    for (auto it = deltas_.begin(); it != deltas_.end(); ++it) {
        s += fmt::format("{}", it->first->toString());
        s += ": ";
        s += fmt::format("{}", it->second->getDelta()->toString());
        if (std::next(it) != deltas_.end())
            s += ", ";
    }

    s += "}, Deltas to cache: {";
    for (auto it = deltaKeysToCache_.begin(); it != deltaKeysToCache_.end(); ++it) {
        s += fmt::format("{}", it->get()->toString());
        if (std::next(it) != deltaKeysToCache_.end()) {
            s += ", ";
        }
    }
    s += "}";

    return s;*/
}