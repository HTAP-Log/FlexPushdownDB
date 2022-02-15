//
// Created by ZhangOscar on 12/3/21.
//

#ifndef HTAP_LOG_CONVERSION_H
#define HTAP_LOG_CONVERSION_H

#include <normal/tuple/TupleSet2.h>
#include <normal/tuple/Scalar.h>

namespace normal::htap::conversion {

    /**
     * Convert the row-oriented deltas into the engine-supported column-oriented delta table
     * @param deltaRows row-oriented deltas
     * @param outputSchema the table schema that the conversion is referencing to
     * @return engine-supported column-oriented delta table
     */
    std::shared_ptr<normal::tuple::TupleSet2> rowToColumn(const std::vector<std::vector<std::shared_ptr<normal::tuple::Scalar>>>& deltaRows, const std::shared_ptr<::arrow::Schema>& outputSchema);

}

#endif //HTAP_LOG_CONVERSION_H