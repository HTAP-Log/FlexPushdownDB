//
// Created by Elena Milkai on 10/14/21.
//

#ifndef NORMAL_CACHETAIL_H
#define NORMAL_CACHETAIL_H

#include <normal/core/Operator.h>
#include <normal/tuple/TupleSet2.h>
#include <string>

namespace normal::htap::deltamanager {
    class CacheTail : public core::Operator {
    public:

        explicit CacheTail(const std::string& OperatorName, const std::string& tableName,  const int &partition,
                           const int &timestamp,  const long queryId);


        static std::shared_ptr<CacheTail> make(const std::string& OperatorName, const std::string& tableName,
                                               const int &partition, const int &timestamp,  const long queryId);

        void onReceive(const core::message::Envelope &msg) override;
        void onStart();

    private:

        std::string tableName_;
        int timestamp_;
        int partition_;

        void readAndSendDeltas(const std::string& tableName, const int partition, const int timestamp = 0);

        std::shared_ptr<normal::tuple::TupleSet2> rowToColumn(std::vector<LineorderDelta_t>& deltaTuples);
    };
}

#endif //NORMAL_CACHETAIL_H
