//
// Created by Elena Milkai on 10/14/21.
//
#ifndef NORMAL_CACHEHANDLER_H
#define NORMAL_CACHEHANDLER_H

#include <normal/core/Operator.h>
#include <normal/tuple/TupleSet2.h>
#include <deltamanager/LoadDeltasRequestMessage.h>
#include <deltamanager/LoadDeltasResponseMessage.h>
#include <deltamanager/StoreTailRequestMessage.h>
#include <normal/core/OperatorContext.h>
#include <normal/connector/partition/Partition.h>
#include <normal/core/message/TupleMessage.h>
#include <string>

using namespace normal::core;

namespace normal::htap::deltamanager {

    class CacheHandler : public core::Operator {
    public:
        explicit CacheHandler(const std::string& OperatorName,
                           const std::string& tableName,
                           const long partition,
                           const long queryId);

        static std::shared_ptr<CacheHandler> make(const std::string& OperatorName,
                                               const std::string& tableName,
                                               const long partition,
                                               const long queryId);

        void onReceive(const core::message::Envelope &msg) override;
        void onStart();

        /**
         * Function executed after CacheHandler receives a LoadDeltasRequestMessage message from DeltaMerge. It passes
         * the message to DeltaCacheActor.
         * @param message
         */
        void OnDeltasRequest(const LoadDeltasRequestMessage &message);

        /**
         * Function executed after CacheHandler receives a ?? message for periodic reading of tail. It passes the
         * message to the DeltaCacheActor.
         * TODO:: need to be implemented
         * @param message
         *
         */
        void OnTailRequest(const StoreTailRequestMessage &message);

        /**
         * Function executed after CacheHandler receives a LoadMessageResponseMessage with requested deltas from the
         * DeltaCacheActor. Then CacheHandler passes the message to DeltaMerge and notifies all that is complete.
         * @param message
         */
        void OnReceiveResponse(const LoadDeltasResponseMessage &message);

    private:
        std::string tableName_;
        long partition_;
    };
}

#endif //NORMAL_CACHEHANDLER_H
