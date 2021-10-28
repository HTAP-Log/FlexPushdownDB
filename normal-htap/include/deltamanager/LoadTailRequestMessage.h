//
// Created by Elena Milkai on 10/28/21.
//

#ifndef NORMAL_LOADTAILREQUESTMESSAGE_H
#define NORMAL_LOADTAILREQUESTMESSAGE_H

#include <normal/core/message/Message.h>
#include <deltamanager/DeltaCacheKey.h>
#include <deltamanager/DeltaCacheData.h>

using namespace normal::core::message;

namespace normal::htap::deltamanager {

    class LoadTailRequestMessage : public Message {

    public:
        LoadTailRequestMessage(std::shared_ptr<DeltaCacheKey> tailKey, const std::string &sender);

        static std::shared_ptr <LoadTailRequestMessage> make(
                const std::shared_ptr<DeltaCacheKey> &tailKey, const std::string &sender);

        [[nodiscard]] const std::shared_ptr<DeltaCacheKey> &getTailKey() const;

        [[nodiscard]] std::string toString() const;

    private:
        std::shared_ptr<DeltaCacheKey> tailKey_;
    };

}

#endif //NORMAL_LOADTAILREQUESTMESSAGE_H
