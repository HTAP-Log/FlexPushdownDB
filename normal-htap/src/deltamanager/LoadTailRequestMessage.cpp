//
// Created by Elena Milkai on 10/28/21.
//

#include <deltamanager/LoadTailRequestMessage.h>

using namespace normal::htap::deltamanager;

LoadTailRequestMessage::LoadTailRequestMessage(std::shared_ptr<DeltaCacheKey> tailKey,
                                               const std::string &sender):
                Message("LoadTailRequestMessage", sender),
                tailKey_(std::move(tailKey)) {}


static std::shared_ptr <LoadTailRequestMessage> LoadTailRequestMessage::make(
        const std::shared_ptr<DeltaCacheKey> &tailKey,
        const std::string &sender) {
    return std::make_shared<LoadTailRequestMessage>(tailKey, sender);
}

[[nodiscard]] const std::shared_ptr<DeltaCacheKey> &LoadTailRequestMessage::getTailKey() const {
    return tailKey_;
}

[[nodiscard]] std::string LoadTailRequestMessage::toString() const {

}
