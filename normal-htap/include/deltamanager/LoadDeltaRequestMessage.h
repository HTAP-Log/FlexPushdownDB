//
// Created by Elena Milkai on 10/25/21.
//

#ifndef NORMAL_LOADDELTAREQUESTMESSAGE_H
#define NORMAL_LOADDELTAREQUESTMESSAGE_H

#include <normal/core/message/Message.h>

#include <deltamanager/DeltaCacheKey.h>
#include <deltamanager/DeltaCacheData.h>

using namespace normal::core::message;

namespace normal::htap::deltamanager {

    class LoadDeltaRequestMessage : public Message {

    public:
        LoadDeltaRequestMessage(std::vector<std::shared_ptr<DeltaCacheKey>> deltaKeys,
                           const std::string &sender);

        static std::shared_ptr<LoadDeltaRequestMessage> make(
                const std::vector<std::shared_ptr<DeltaCacheKey>>& deltaKeys, const std::string &sender);

        [[nodiscard]] const std::vector<std::shared_ptr<DeltaCacheKey>> &getDeltaKeys() const;

        [[nodiscard]] std::string toString() const;

    private:
        std::vector<std::shared_ptr<DeltaCacheKey>> deltaKeys_;

    };
}

#endif //NORMAL_LOADDELTAREQUESTMESSAGE_H
