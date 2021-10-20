//
// Created by Elena Milkai on 11/8/21.
//

#ifndef NORMAL_LOADDELTASREQUESTMESSAGE_H
#define NORMAL_LOADDELTASREQUESTMESSAGE_H

#include <deltamanager/DeltaCache.h>
#include <deltamanager/DeltaCacheKey.h>
#include <normal/core/message/Message.h>

using namespace normal::core::message;

namespace normal::htap::deltamanager {

    class LoadDeltasRequestMessage : public Message {

    public:
        LoadDeltasRequestMessage(std::shared_ptr<DeltaCacheKey> deltaKey,
                                 const std::string &sender);

        static std::shared_ptr <LoadDeltasRequestMessage> make(
                const std::shared_ptr<DeltaCacheKey> &deltaKey, const std::string &sender);

        [[nodiscard]] const std::shared_ptr<DeltaCacheKey>& getDeltaKey() const;

        [[nodiscard]] std::string toString() const;

    private:
        //TODO:: change the vector
        std::shared_ptr<DeltaCacheKey> deltaKey_;

    };
}
#endif //NORMAL_LOADDELTASREQUESTMESSAGE_H
