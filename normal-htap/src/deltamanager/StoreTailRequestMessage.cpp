//
// Created by Elena Milkai on 11/7/21.
//

#include <deltamanager/StoreTailRequestMessage.h>

using namespace normal::htap::deltamanager;

StoreTailRequestMessage::StoreTailRequestMessage(std::shared_ptr<DeltaCacheKey> tailKey,
                                                 const std::string &sender):
        Message("StoreTailRequestMessage", sender),
        tailKey_(std::move(tailKey)) {}


std::shared_ptr <StoreTailRequestMessage> StoreTailRequestMessage::make(
        std::shared_ptr<DeltaCacheKey> &tailKey,
        const std::string &sender) {
    return std::make_shared<StoreTailRequestMessage>(tailKey, sender);
}

[[nodiscard]] const std::shared_ptr<DeltaCacheKey>& StoreTailRequestMessage::getTailKey() const {
    return tailKey_;
}
