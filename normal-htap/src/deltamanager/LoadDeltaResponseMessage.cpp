//
// Created by Elena Milkai on 10/25/21.
//

#include <deltamanager/LoadDeltaResponseMessage.h>


using namespace normal::htap::deltamanager;

LoadDeltaResponseMessage::LoadDeltaResponseMessage(std::unordered_map<std::shared_ptr<DeltaCacheKey>,
                                                                      std::shared_ptr<DeltaCacheData>,
                                                                      DeltaKeyPointerHash,
                                                                      DeltaKeyPointerPredicate> deltas,
                                                                      const std::string &sender,
                           std::vector<std::shared_ptr<DeltaCacheKey>> deltaKeysToCache):
                           Message("LoadDeltaResponseMessage", sender),
                           deltas_(std::move(deltas)),
                           deltaKeysToCache_(std::move(deltaKeysToCache)){}

std::shared_ptr<LoadDeltaResponseMessage> LoadDeltaResponseMessage::make(std::unordered_map<std::shared_ptr<DeltaCacheKey>,
                                                                         std::shared_ptr<DeltaCacheData>,
                                                                         DeltaKeyPointerHash,
                                                                         DeltaKeyPointerPredicate> deltas,
                                                                         const std::string &sender,
                std::vector<std::shared_ptr<DeltaCacheKey>> deltaKeysToCache){
    return std::make_shared<LoadDeltaResponseMessage>(std::move(deltas), sender, std::move(deltaKeysToCache));
}

[[maybe_unused]] [[nodiscard]] const std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>,
        DeltaKeyPointerHash, DeltaKeyPointerPredicate> &LoadDeltaResponseMessage::getDeltas() const {
    return deltas_;
}

[[nodiscard]] const std::vector<std::shared_ptr<DeltaCacheKey>> &LoadDeltaResponseMessage::getDeltaKeysToCache() const {
    return deltaKeysToCache_;
}

[[nodiscard]] std::string LoadDeltaResponseMessage::toString() const {
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