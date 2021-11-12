//
// Created by Elena Milkai on 10/14/21.
//
// ------CacheHandler potentially used later--------
#ifndef NORMAL_CACHEHANDLER_H
#define NORMAL_CACHEHANDLER_H

#include <normal/core/Operator.h>
#include <normal/tuple/TupleSet2.h>
#include <deltamanager/LoadDeltasRequestMessage.h>
#include <deltamanager/LoadDeltasResponseMessage.h>
#include <deltamanager/StoreTailRequestMessage.h>
#include <normal/core/OperatorContext.h>
#include <string>

using namespace normal::core;

namespace normal::htap::deltamanager {

    class CacheHandler : public core::Operator {
    public:
        explicit CacheHandler(const std::string& OperatorName,
                           const std::string& tableName,
                           const int &partition,
                           const int &timestamp,
                           const long queryId);

        static std::shared_ptr<CacheHandler> make(const std::string& OperatorName,
                                               const std::string& tableName,
                                               const int &partition,
                                               const int &timestamp,
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
         * @param message
         * Function executed after CacheHandler receives a ?? message for periodic reading of tail. It passes the
         * message to the DeltaCacheActor.
         * TODO:: need to be implemented
         */
        void OnTailRequest(const StoreTailRequestMessage &message);

        /**
         * @param deltaKeys
         * Function that receives the memory deltas and tail from the DeltaCacheActor and sends response to
         * the DeltaMerge operator.
         */
        void OnDeltasResponse(const LoadDeltasResponseMessage &message);

    private:
        std::string tableName_;
        int timestamp_;
        int partition_;
    };
}

#endif //NORMAL_CACHEHANDLER_H
