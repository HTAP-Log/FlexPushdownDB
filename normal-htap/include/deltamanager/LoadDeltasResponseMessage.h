//
// Created by Elena Milkai on 10/28/21.
//

#ifndef NORMAL_LOADDELTASRESPONSEMESSAGE_H
#define NORMAL_LOADDELTASRESPONSEMESSAGE_H

#include <normal/core/message/Message.h>
#include <deltamanager/DeltaCacheKey.h>
#include <deltamanager/DeltaCacheData.h>
#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;
using namespace normal::core::message;

namespace normal::htap::deltamanager {

/**
 * Response sent from delta cache actor containing deltas loaded from cache, deltas data is nullopt if delta
 * was not found in cache.
 */
    class LoadDeltasResponseMessage : public Message {

    public:
        LoadDeltasResponseMessage(std::vector<std::shared_ptr<TupleSet2>> deltas,
                                  std::vector<std::shared_ptr<int>> timestamps,
                                  const std::string &sender);

        static std::shared_ptr<LoadDeltasResponseMessage> make(std::vector<std::shared_ptr<TupleSet2>> deltas,
                                                               std::vector<std::shared_ptr<int>> timestamps,
                                                               const std::string &sender);

        const std::vector<std::shared_ptr<TupleSet2>> &getDeltas() const;

        const std::vector<std::shared_ptr<int>> &getTimestamps() const;

        [[nodiscard]] std::string toString() const;

    private:
        std::vector<std::shared_ptr<TupleSet2>>  deltas_;
        std::vector<std::shared_ptr<int>> timestamps_;

    };
}
#endif //NORMAL_LOADDELTASRESPONSEMESSAGE_H
