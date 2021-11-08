//
// Created by Elena Milkai on 10/28/21.
//

#ifndef NORMAL_LOADTAILRESPONSEMESSAGE_H
#define NORMAL_LOADTAILRESPONSEMESSAGE_H

#include <normal/core/message/Message.h>
#include <deltamanager/DeltaCacheKey.h>
#include <deltamanager/DeltaCacheData.h>

using namespace normal::core::message;

namespace normal::htap::deltamanager {

    class LoadTailResponseMessage : public Message {

    public:
        LoadTailResponseMessage(std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>> deltas,
                                 const std::string &sender);

        static std::shared_ptr<LoadTailResponseMessage>
        make(const std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>>& deltas,
             const std::string &sender);

        [[nodiscard]] const std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>> &
        getTailDelta() const;

        [[nodiscard]] std::string toString() const;

    private:
        std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>> tail_;
    };
}

#endif //NORMAL_LOADTAILRESPONSEMESSAGE_H
