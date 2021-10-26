//
// Created by Elena Milkai on 10/25/21.
//

#include <deltamanager/StoreDeltaRequestMessage.h>

using namespace normal::htap::deltamanager;


StoreDeltaRequestMessage::StoreDeltaRequestMessage(
        std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>> deltas,
        const std::string &sender) :
        Message("StoreRequestMessage", sender),
        deltas_(std::move(deltas)) {}

std::shared_ptr<StoreDeltaRequestMessage>
StoreDeltaRequestMessage::make(const std::unordered_map<std::shared_ptr<DeltaCacheKey>,
        std::shared_ptr<DeltaCacheData>>& deltas,
        const std::string &sender) {
    return std::make_shared<StoreDeltaRequestMessage>(deltas, sender);
}

[[nodiscard]] const std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>> &
StoreDeltaRequestMessage::getTailDelta() const {
    return deltas_;
}

[[nodiscard]] std::string StoreDeltaRequestMessage::toString() const {
    /*std::string s = "{";
    for (auto it = deltas_.begin(); it != deltas_.end(); ++it) {
        s += fmt::format("{}", it->first->toString());
        s += ": ";
        s += fmt::format("{}", it->second->getDelta());
        if (std::next(it) != deltas_.end())
            s += ", ";
    }
    s += "}";

    return s;*/

}