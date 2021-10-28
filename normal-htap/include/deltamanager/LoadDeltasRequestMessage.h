//
// Created by Elena Milkai on 10/28/21.
//

#ifndef NORMAL_LOADDELTASREQUESTMESSAGE_H
#define NORMAL_LOADDELTASREQUESTMESSAGE_H

#include <deltamanager/DeltaCache.h>
#include <deltamanager/DeltaCacheKey.h>

using namespace normal::core::message;

namespace normal::htap::deltamanager {

    class LoadDeltasRequestMessage : public Message {

    public:
        LoadDeltasRequestMessage(std::vector <std::shared_ptr<DeltaCacheKey>> deltaKeys,
                                 const std::string &sender);

        static std::shared_ptr <LoadDeltasRequestMessage> make(
                const std::vector <std::shared_ptr<DeltaCacheKey>> &deltaKeys, const std::string &sender);

        [[nodiscard]] const std::vector <std::shared_ptr<DeltaCacheKey>> &getDeltaKeys() const;

        [[nodiscard]] std::string toString() const;

    private:
        std::vector <std::shared_ptr<DeltaCacheKey>> deltaKeys_;

    };
}
#endif //NORMAL_LOADDELTASREQUESTMESSAGE_H
