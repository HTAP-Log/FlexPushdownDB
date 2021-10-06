//
// Created by ZhangOscar on 10/5/21.
//

#ifndef NORMAL_GETTAILDELTAS_H
#define NORMAL_GETTAILDELTAS_H

#include <normal/core/Operator.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/core/message/TupleMessage.h>
#include <normal/tuple/TupleSet2.h>

#include <BinlogParser.h>

#include <string>

using namespace normal::avro_tuple::make;

namespace normal::htap::deltamanager {
    class GetTailDeltas : public core::Operator {
    public:

        /**
         * Explicit constructor
         * @param OperatorName
         * @param queryId from the analytical query for foreground merging (to answer the query result)
         */
        explicit GetTailDeltas(const std::string& OperatorName, const long queryId);

        /**
         *
         * @param tableName table associated with the foreground merging
         * @param OperatorName
         * @param queryId
         * @param outputSchema the schema we need to output from the chain of the operator
         */
        GetTailDeltas(const std::string& tableName, const std::string& OperatorName, const long queryId, std::shared_ptr<::arrow::Schema>  outputSchema);

        GetTailDeltas(const std::string& tableName, const std::string& OperatorName, const long queryId);

        static std::shared_ptr<GetTailDeltas> make(const std::string& OperatorName, const long queryId);

        static std::shared_ptr<GetTailDeltas> make(const std::string& tableName, const std::string& OperatorName, const long queryId, const std::shared_ptr<::arrow::Schema>& outputSchema);

        void onReceive(const core::message::Envelope &msg) override;

        /**
         * This function will call ctx->complete() and notifyAll by itself
         */
        void onStart();

    private:

        std::string tableName_;
        std::shared_ptr<::arrow::Schema> outputSchema_;
        int timestamp_;
        int partition_;

        /**
         * Get relevant deltas from binlog
         * TODO: we also need timestamp from the query, this should be a functionality of deltapump.
         * @param tableName the table that we are getting
         * @param partition the partition that we are getting out of this table
         * @return column-oriented delta data
         */
        void readAndSendDeltas(const std::string& tableName, const int partition, const int timestamp = 0);


        /**
         * Helper function that converts row-oriented deltas to column-oriented deltas
         * TODO: we need to be able to get a list of builders from metadata API
         * @param deltaTuples row-oriented deltas
         * @return column-oriented deltas
         */
        std::shared_ptr<normal::tuple::TupleSet2> rowToColumn(std::vector<LineorderDelta_t>& deltaTuples);
    };
}

#endif //NORMAL_GETTAILDELTAS_H
