//
// Created by ZhangOscar on 10/5/21.
//

#include <deltamanager/GetTailDeltas.h>

std::shared_ptr<std::map<int, std::shared_ptr<normal::tuple::TupleSet2>>> normal::htap::deltamanager::callDeltaPump(const std::shared_ptr<::arrow::Schema>& outputSchema) {
    // now call binlog parser API
    std::unordered_map<int, std::set<struct lineorder_record>> *lineorder_record_ptr = nullptr;
    // TODO: change this hardcoded method
    const char* path = "./bin.000002"; //binlog file path
    const char* path_range = "./partitions/ranges.csv"; //range file path
    BinlogParser binlogParser;
    binlogParser.parse(path, path_range, &lineorder_record_ptr);
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
    long arraySize = deltaTuples.size();
    // TODO: we need to be able to get a list of builders from metadata API
    // Right now just hardcode all the builders

    std::unordered_map<int, std::vector<int32_t>> int32Cols;
    std::unordered_map<int, std::vector<int64_t>> int64Cols;
    std::unordered_map<int, std::vector<std::string>> stringCols;
    std::unordered_map<BuilderDataType, std::vector<int>> builderIndices = {
            {Int32, {0, 1, 2, 3, 4, 5, 8, 9, 10, 11, 14, 15, 18}},
            {String, {6, 7, 16, 17}},
            {Int64, {12, 13}}
    };
    std::unordered_map<int, std::shared_ptr<arrow::Int32Builder>> int32Builders = {};
    std::unordered_map<int, std::shared_ptr<arrow::Int64Builder>> int64Builders = {};
    std::unordered_map<int, std::shared_ptr<arrow::StringBuilder>> stringBuilders = {};
    std::vector<std::shared_ptr<arrow::Array>> arrowArrays(19);

    // There is a workaround to access std::tuple elements dynamically, but not necessary right now since we are hardcoding everything
    for (auto t : deltaTuples) {
        int32Cols[0].emplace_back( std::get<0>(t));
        int32Cols[1].emplace_back( std::get<1>(t));
        int32Cols[2].emplace_back( std::get<2>(t));
        int32Cols[3].emplace_back( std::get<3>(t));
        int32Cols[4].emplace_back( std::get<4>(t));
        int32Cols[5].emplace_back( std::get<5>(t));
        int32Cols[8].emplace_back( std::get<8>(t));
        int32Cols[9].emplace_back( std::get<9>(t));
        int32Cols[10].emplace_back( std::get<10>(t));
        int32Cols[11].emplace_back( std::get<11>(t));
        int32Cols[14].emplace_back( std::get<14>(t));
        int32Cols[15].emplace_back( std::get<15>(t));
        int32Cols[18].emplace_back( std::get<18>(t));
        int64Cols[12].emplace_back( std::get<12>(t));
        int64Cols[13].emplace_back( std::get<13>(t));
        stringCols[6].emplace_back(std::get<6>(t));
        stringCols[7].emplace_back(std::get<7>(t));
        stringCols[16].emplace_back(std::get<16>(t));
        stringCols[17].emplace_back(std::get<17>(t));
    }

    arrow::MemoryPool* pool = arrow::default_memory_pool();
    // Initialize all required builders according to builderIndices
    for (std::pair<BuilderDataType, std::vector<int>> indices : builderIndices) {
        for (std::size_t index : indices.second) {
            switch (indices.first) {
                case Int32:
                    int32Builders[index] = std::make_shared<arrow::Int32Builder>(pool);
                    int32Builders[index]->Resize(arraySize);
                    int32Builders.at(index)->AppendValues(int32Cols.at(index));
                    if (!int32Builders[index]->Finish(&arrowArrays[index]).ok()) {
                        throw std::runtime_error(fmt::format("Error building arrow array from delta tuples"));
                    }
                    break;
                case Int64:
                    int64Builders[index] = std::make_shared<arrow::Int64Builder>(pool);
                    int64Builders[index]->Resize(arraySize);
                    int64Builders.at(index)->AppendValues(int64Cols.at(index));
                    if (!int64Builders[index]->Finish(&arrowArrays[index]).ok()) {
                        throw std::runtime_error(fmt::format("Error building arrow array from delta tuples"));
                    }
                    break;
                case String:
                    stringBuilders[index] = std::make_shared<arrow::StringBuilder>(pool);
                    stringBuilders[index]->Resize(arraySize);
                    stringBuilders.at(index)->AppendValues(stringCols.at(index));
                    if (!stringBuilders[index]->Finish(&arrowArrays[index]).ok()) {
                        throw std::runtime_error(fmt::format("Error building arrow array from delta tuples"));
                    }
                    break;
            }
        }
    }

    // TODO: check the schema is correct, now we just assume it is correct
    return TupleSet2::make(outputSchema, arrowArrays);
}