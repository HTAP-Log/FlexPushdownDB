//
// Created by Elena Milkai on 10/14/21.
//

#ifndef NORMAL_CACHEHANDLER_H
#define NORMAL_CACHEHANDLER_H

#include <normal/core/Operator.h>
#include <normal/tuple/TupleSet2.h>
#include <deltamanager/LoadDeltasRequestMessage.h>
//#include <deltamanager/LoadTailRequestMessage.h>
//#include <deltamanager/LoadTailResponseMessage.h>
#include <deltamanager/LoadDeltasResponseMessage.h>
#include <string>

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
         * @param message
         * When the CacheHandler receives a LoadDeltasRequest msg from the DeltaMerge then, it checks If the requested
         * deltas' keys are already in memory. If yes, the CacheHandler sends a LoadDeltasResponse msg to
         * DeltaMerge with the requested deltas. If not, then the CacheHandler send a LoadTailRequest msg to the
         * GetTailDeltas.
         */
        void OnLoadDeltas(const LoadDeltasRequestMessage &message);
        /**
         * @param message
         * When the CacheHandler receives a LoadTailResponse msg from the GetTailDeltas then, it first stores the tail
         * delta in memory. Next, the CacheHandler collects all the deltas and send a LoadDeltasResponse msg to the
         * DeltaMerge operator.
         */
        void OnTailResponse(const LoadTailResponseMessage &message);

        /**
         * @param deltaKeys
         * Accesses memory deltas and sends response with the deltas to the DeltaMerge operator.
         */
        void sendResponse(const std::vector<std::shared_ptr<DeltaCacheKey>> &deltaKeys);

    private:
        std::string tableName_;
        int timestamp_;
        int partition_;
    };
}

#endif //NORMAL_CACHEHANDLER_H
