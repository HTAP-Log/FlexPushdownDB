//
// Created by ZhangOscar on 10/5/21.
//

#ifndef NORMAL_GETTAILDELTAS_H
#define NORMAL_GETTAILDELTAS_H
#include <normal/core/Operator.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/core/message/TupleMessage.h>
#include <normal/tuple/TupleSet2.h>

#include <string>
#include "../../../normal-deltapump/include/deltapump/makeTuple.h" // TODO: change the import directory
#include "BinlogParser.h"

using namespace normal::avro_tuple::make;

/**
 * This file contains helper functions to call DeltaPump-exposed APIs and do necessary conversions
 * from row-oriented data format to column-oriented data format.
 */
namespace normal::htap::deltamanager {

    enum BuilderDataType {Int32, Int64, String};

    /**
     * Calls DeltaPump API to parse the binlog
     * @return a map from partition number to a specific column-oriented delta table (Arrow) parsed from tail of the log
     */
    std::shared_ptr<std::map<int, std::shared_ptr<normal::tuple::TupleSet2>>> callDeltaPump(const std::shared_ptr<::arrow::Schema>& outputSchema);

    /**
     * Converts the row-oriented delta table to column-oriented delta table (Arrow)
     * @param deltaTuples the row-oriented delta table given by DeltaPump
     * @return column-oriented delta table (Arrow)
     */
    std::shared_ptr<normal::tuple::TupleSet2> rowToColumn(const std::vector<LineorderDelta_t>& deltaTuples, const std::shared_ptr<::arrow::Schema>& outputSchema);

}

#endif //NORMAL_GETTAILDELTAS_H
