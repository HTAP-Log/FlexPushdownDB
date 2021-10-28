//
// Created by Elena Milkai on 10/28/21.
//

//
// Created by Elena Milkai on 10/25/21.
//

#include <deltamanager/LoadTailResponseMessage.h>

using namespace normal::htap::deltamanager;

LoadTailResponseMessage::LoadTailResponseMessage(
        std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>> deltas,
        const std::string &sender) :
        Message("LoadTailResponseMessage", sender),
        deltas_(std::move(deltas)) {}

std::shared_ptr<LoadTailResponseMessage>
LoadTailResponseMessage::make(const std::unordered_map<std::shared_ptr<DeltaCacheKey>,
        std::shared_ptr<DeltaCacheData>>& deltas,
                               const std::string &sender) {
    return std::make_shared<LoadTailResponseMessage>(deltas, sender);
}

[[nodiscard]] const std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>> &
LoadTailResponseMessage::getTailDelta() const {
    return deltas_;
}

[[nodiscard]] std::string LoadTailResponseMessage::toString() const {
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