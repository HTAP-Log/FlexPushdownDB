//
// Created by Elena Milkai on 10/25/21.
//

#ifndef NORMAL_STOREDELTAREQUESTMESSAGE_H
#define NORMAL_STOREDELTAREQUESTMESSAGE_H

#include <normal/core/message/Message.h>

#include <deltamanager/DeltaCacheKey.h>
#include <deltamanager/DeltaCacheData.h>

using namespace normal::core::message;

namespace normal::htap::deltamanager {

    class StoreDeltaRequestMessage : public Message {

    public:
        StoreDeltaRequestMessage(std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>> deltas,
                            const std::string &sender);

        static std::shared_ptr<StoreDeltaRequestMessage>
        make(const std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>>& deltas,
             const std::string &sender);

        [[nodiscard]] const std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>> &
        getTailDelta() const;

        [[nodiscard]] std::string toString() const;

    private:
        std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>> deltas_;
    };

}

#endif //NORMAL_STOREDELTAREQUESTMESSAGE_H
