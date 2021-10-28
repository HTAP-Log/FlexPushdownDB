//
// Created by Elena Milkai on 10/14/21.
//

#ifndef NORMAL_CACHEHANDLER_H
#define NORMAL_CACHEHANDLER_H

#include <normal/core/Operator.h>
#include <normal/tuple/TupleSet2.h>
#include <deltamanager/LoadDeltasRequestMessage.h>
#include <deltamanager/LoadTailRequestMesssage.h>
#include <deltamanager/LoadTailResponseMessage.h>
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
        void OnLoadDeltas(const LoadDeltasRequestMessage &message);
        void OnTailResponse(const LoadTailResponseMessage &message);

    private:
        std::string tableName_;
        int timestamp_;
        int partition_;
    };
}

#endif //NORMAL_CACHEHANDLER_H
