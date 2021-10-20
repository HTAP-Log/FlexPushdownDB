//
// Created by Elena Milkai on 11/7/21.
//

#ifndef NORMAL_STORETAILREQUESTMESSAGE_H
#define NORMAL_STORETAILREQUESTMESSAGE_H

#include <normal/core/message/Message.h>
#include <deltamanager/DeltaCacheKey.h>
#include <deltamanager/DeltaCacheData.h>

using namespace normal::core::message;

namespace normal::htap::deltamanager {

    class StoreTailRequestMessage : public Message {
    public:
        StoreTailRequestMessage(std::shared_ptr<DeltaCacheKey> tailKey,
                                const std::string &sender);

        static std::shared_ptr<StoreTailRequestMessage>
        make(std::shared_ptr<DeltaCacheKey> &tailKey,
             const std::string &sender);

        [[nodiscard]] const std::shared_ptr<DeltaCacheKey>& getTailKey() const;

    private:
        std::shared_ptr<DeltaCacheKey> tailKey_;
    };
}

#endif //NORMAL_STORETAILREQUESTMESSAGE_H
