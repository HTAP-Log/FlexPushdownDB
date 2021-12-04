//
// Created by ZhangOscar on 10/5/21.
//

#include <deltamanager/GetTailDeltas.h>
#include <normal/tuple/ArrayAppender.h>
#include <normal/tuple/ArrayAppenderWrapper.h>
#include <normal/tuple/ColumnBuilder.h>

BinlogParser binlogParser; // Global java object enforced by JVM initialization

std::shared_ptr<std::map<int, std::shared_ptr<normal::tuple::TupleSet2>>> normal::htap::deltamanager::callDeltaPump(const std::shared_ptr<::arrow::Schema>& outputSchema) {
    // now call binlog parser API
    std::unordered_map<int, std::set<struct lineorder_record>> *lineorder_record_ptr = nullptr;
    //the 5 variables below are used for later E2E test on all tables
    std::unordered_map<int, std::set<struct customer_record>> *customer_record_ptr = nullptr;
    std::unordered_map<int, std::set<struct supplier_record>> *supplier_record_ptr = nullptr;
    std::unordered_map<int, std::set<struct part_record>> *part_record_ptr = nullptr;
    std::unordered_map<int, std::set<struct date_record>> *date_record_ptr = nullptr;
    // TODO: change this hardcoded method and remove parameter path
    const char* path = "/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/bin.000002"; //binlog file path
    // BinlogParser binlogParser;
    binlogParser.parse(path, &lineorder_record_ptr, &customer_record_ptr, &supplier_record_ptr, &part_record_ptr, &date_record_ptr);
    SPDLOG_DEBUG("##### After parsing #####");
    if (lineorder_record_ptr == nullptr) {
        throw std::runtime_error(fmt::format("Error parsing binlog"));
    }

    std::map<int, std::shared_ptr<normal::tuple::TupleSet2>> deltaRecords;
    for (const auto& lineorder_partition_pair : (*lineorder_record_ptr)) {
        int part = lineorder_partition_pair.first;
        std::set<struct lineorder_record> record_set = lineorder_partition_pair.second;
        std::vector<LineorderDelta_t> record_table(record_set.size());
        for (const auto &record: record_set) {
            record_table.emplace_back(record.lineorder_delta);
        }
        std::shared_ptr<normal::tuple::TupleSet2> column_table = normal::htap::deltamanager::rowToColumn(record_table, outputSchema);
        deltaRecords[part] = column_table;
    }
    return std::make_shared<std::map<int, std::shared_ptr<normal::tuple::TupleSet2>>>(deltaRecords);
}

std::shared_ptr<normal::tuple::TupleSet2> normal::htap::deltamanager::rowToColumn(const std::vector<LineorderDelta_t>& deltaTuples, const std::shared_ptr<::arrow::Schema>& outputSchema) {
////    // initialize an array of column builders
////    std::vector<std::shared_ptr<ColumnBuilder>> builders(outputSchema->num_fields());
////    for (int i = 0; i < outputSchema->num_fields(); i ++) {
////        std::string builderName = outputSchema->field_names()[i] + "_builder";
////        builders[i] = ColumnBuilder::make(builderName, outputSchema->field(i)->type());
////    }
////
////    // append all the columns row by row
////    const size_t numRows = deltaTuples.size();
////    const size_t numCols = builders.size();
////    for (size_t i = 0; i < numRows; i ++) {
////        for (size_t j = 0; j < numCols; j ++) {
////            LineorderDelta_t currRow = deltaTuples[i];
////            auto value = std::get<j>(currRow);
////            builders[j]->append()
////        }
//    }
    return nullptr;
}