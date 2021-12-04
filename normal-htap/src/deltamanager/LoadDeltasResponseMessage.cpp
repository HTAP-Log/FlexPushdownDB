//
// Created by Elena Milkai on 10/25/21.
//

#include <deltamanager/LoadDeltasResponseMessage.h>

using namespace normal::htap::deltamanager;

LoadDeltasResponseMessage::LoadDeltasResponseMessage(std::vector<std::shared_ptr<TupleSet2>> deltas,
                                                     std::vector<std::shared_ptr<int>> timestamps,
                                                     const std::string &sender):
                           Message("LoadDeltasResponseMessage", sender),
                           deltas_(std::move(deltas)),
                           timestamps_(std::move(timestamps)){}

std::shared_ptr<LoadDeltasResponseMessage> LoadDeltasResponseMessage::make(std::vector<std::shared_ptr<TupleSet2>> deltas,
                                                                           std::vector<std::shared_ptr<int>> timestamps,
                                                                           const std::string &sender){
    return std::make_shared<LoadDeltasResponseMessage>(std::move(deltas), std::move(timestamps), sender);
}

const std::vector<std::shared_ptr<TupleSet2>>& LoadDeltasResponseMessage::getDeltas() const {
    return deltas_;
}

const std::vector<std::shared_ptr<int>>& LoadDeltasResponseMessage::getTimestamps() const {
    return timestamps_;
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