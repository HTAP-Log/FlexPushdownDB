//
// Created by ZhangOscar on 12/3/21.
//

#include <conversion/Conversion.h>
#include <normal/tuple/ArrayAppender.h>
#include <normal/tuple/ColumnBuilder.h>

using namespace normal::htap::conversion;
using namespace normal::tuple;

std::shared_ptr<normal::tuple::TupleSet2> rowToColumn(const std::vector<std::vector<std::shared_ptr<normal::tuple::Scalar>>>& deltaRows, const std::shared_ptr<::arrow::Schema>& outputSchema) {
    // initialize an array of column builders
    std::vector<std::shared_ptr<normal::tuple::ColumnBuilder>> builders(outputSchema->num_fields());
    for (int i = 0; i < outputSchema->num_fields(); i ++) {
        std::string builderName = outputSchema->field_names()[i] + "_builder";
        builders[i] = normal::tuple::ColumnBuilder::make(builderName, outputSchema->field(i)->type());
    }

    // append all the columns row by row
    const size_t numRows = deltaRows.size();
    const size_t numCols = builders.size();
    for (size_t i = 0; i < numRows; i ++) {
        for (size_t j = 0; j < numCols; j ++) {
            std::shared_ptr<normal::tuple::Scalar> value = deltaRows[i][j];
            builders[i]->append(value);
        }
    }

    // finalize the columns from builders
    std::vector<std::shared_ptr<Column>> builtColumns;
    for (auto & builder : builders) {
        builtColumns.emplace_back(builder->finalize());
    }

    // making the final table
    auto deltaTable = normal::tuple::TupleSet2::make(normal::tuple::Schema::make(outputSchema), builtColumns);
    return deltaTable;
}
