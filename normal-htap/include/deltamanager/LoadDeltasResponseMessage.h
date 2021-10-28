//
// Created by Elena Milkai on 10/28/21.
//

#ifndef NORMAL_LOADDELTASRESPONSEMESSAGE_H
#define NORMAL_LOADDELTASRESPONSEMESSAGE_H

#include <normal/core/message/Message.h>
#include <deltamanager/DeltaCacheKey.h>
#include <deltamanager/DeltaCacheData.h>

using namespace normal::core::message;

namespace normal::htap::deltamanager {

/**
 * Response sent from delta cache actor containing deltas loaded from cache, deltas data is nullopt if delta
 * was not found in cache.
 */
    class LoadDeltasResponseMessage : public Message {

    public:
        LoadDeltasResponseMessage(std::unordered_map<std::shared_ptr<DeltaCacheKey>,
                std::shared_ptr<DeltaCacheData>,
                DeltaKeyPointerHash,
                DeltaKeyPointerPredicate> deltas,
                                  const std::string &sender,
                                  std::vector<std::shared_ptr<DeltaCacheKey>> deltaKeysToCache);

        static std::shared_ptr<LoadDeltasResponseMessage> make(std::unordered_map<std::shared_ptr<DeltaCacheKey>,
                std::shared_ptr<DeltaCacheData>,
                DeltaKeyPointerHash,
                DeltaKeyPointerPredicate> deltas,
                                                               const std::string &sender,
                                                               std::vector<std::shared_ptr<DeltaCacheKey>> deltaKeysToCache);

        [[maybe_unused]] [[nodiscard]] const std::unordered_map<std::shared_ptr<DeltaCacheKey>,
                std::shared_ptr<DeltaCacheData>,
                DeltaKeyPointerHash,
                DeltaKeyPointerPredicate> &getDeltas() const;

        [[nodiscard]] const std::vector<std::shared_ptr<DeltaCacheKey>> &getDeltaKeysToCache() const;

        [[nodiscard]] std::string toString() const;

    private:
        std::unordered_map<std::shared_ptr<DeltaCacheKey>,
                std::shared_ptr<DeltaCacheData>,
                DeltaKeyPointerHash,
                DeltaKeyPointerPredicate> deltas_;
        std::vector<std::shared_ptr<DeltaCacheKey>> deltaKeysToCache_;

    };
}
#endif //NORMAL_LOADDELTASRESPONSEMESSAGE_H
