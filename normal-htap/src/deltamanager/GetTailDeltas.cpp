//
// Created by ZhangOscar on 10/5/21.
//

#include <deltamanager/GetTailDeltas.h>

BinlogParser binlogParser; // Global java object enforced by JVM initialization

std::shared_ptr<std::map<int, std::shared_ptr<normal::tuple::TupleSet2>>> normal::htap::deltamanager::callDeltaPump(const std::shared_ptr<::arrow::Schema>& outputSchema) {
    // now call binlog parser API
    std::unordered_map<int, std::set<struct lineorder_record>> *lineorder_record_ptr = nullptr;
    //the 5 variables below are used for later E2E test on all tables
    std::unordered_map<int, std::set<struct customer_record>> *customer_record_ptr = nullptr;
    std::unordered_map<int, std::set<struct supplier_record>> *supplier_record_ptr = nullptr;
    std::unordered_map<int, std::set<struct part_record>> *part_record_ptr = nullptr;
    std::unordered_map<int, std::set<struct date_record>> *date_record_ptr = nullptr;
    // TODO: change this hardcoded method
    const char* path = "/home/ubuntu/FPDB_oscar/cmake-build-debug-aws-htap/normal-deltapump/bin.000002"; //binlog file path
    // BinlogParser binlogParser;
//    binlogParser.parse(path, path_range, &lineorder_record_ptr);
    //SPDLOG_CRITICAL("#### CALLING Deltapump!!!!");
    binlogParser.parse(path, &lineorder_record_ptr, &customer_record_ptr, &supplier_record_ptr, &part_record_ptr, &date_record_ptr);

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
    arrow::Int32Builder builder0, builder1, builder2, builder3, builder4, builder5, builder8, builder9, builder10, builder11, builder14, builder15, builder18;
    arrow::StringBuilder builder6, builder7, builder16, builder17;
    arrow::Int64Builder builder12, builder13;

    // Initialize builder size for bulk processing
    std::vector<arrow::Int32Builder*> int32Group = {&builder0, &builder1, &builder2, &builder3, &builder4, &builder5, &builder8, &builder9, &builder10, &builder11, &builder14, &builder15, &builder18};
    std::vector<arrow::Int64Builder*> int64Group = {&builder12, &builder13};
    std::vector<arrow::StringBuilder*> stringGroup = {&builder6, &builder7, &builder16, &builder17};
    for (auto builder : int32Group) builder->Resize(arraySize);
    for (auto builder : int64Group) builder->Resize(arraySize);
    for (auto builder : stringGroup) builder->Resize(arraySize);

    std::vector<int32_t> col0, col1, col2, col3, col4, col5, col8, col9, col10, col11, col14, col15, col18; // all ints
    std::vector<int64_t> col12, col13;
    std::vector<std::string> col6, col7, col16, col17;
    for (auto t : deltaTuples) {
        col0.emplace_back(std::get<0>(t));
        col1.emplace_back(std::get<1>(t));
        col2.emplace_back(std::get<2>(t));
        col3.emplace_back(std::get<3>(t));
        col4.emplace_back(std::get<4>(t));
        col5.emplace_back(std::get<5>(t));
        col6.emplace_back(std::get<6>(t));
        col7.emplace_back(std::get<7>(t));
        col8.emplace_back(std::get<8>(t));
        col9.emplace_back(std::get<9>(t));
        col10.emplace_back(std::get<10>(t));
        col11.emplace_back(std::get<11>(t));
        col12.emplace_back(std::get<12>(t));
        col13.emplace_back(std::get<13>(t));
        col14.emplace_back(std::get<14>(t));
        col15.emplace_back(std::get<15>(t));
        col16.emplace_back(std::get<16>(t));
        col17.emplace_back(std::get<17>(t));
        col18.emplace_back(std::get<18>(t));
    }
    builder0.AppendValues(col0);
    builder1.AppendValues(col1);
    builder2.AppendValues(col2);
    builder3.AppendValues(col3);
    builder4.AppendValues(col4);
    builder5.AppendValues(col5);
    builder6.AppendValues(col6);
    builder7.AppendValues(col7);
    builder8.AppendValues(col8);
    builder9.AppendValues(col9);
    builder10.AppendValues(col10);
    builder11.AppendValues(col11);
    builder12.AppendValues(col12);
    builder13.AppendValues(col13);
    builder14.AppendValues(col14);
    builder15.AppendValues(col15);
    builder16.AppendValues(col16);
    builder17.AppendValues(col17);
    builder18.AppendValues(col18);

    std::shared_ptr<arrow::Array> arr0, arr1, arr2, arr3, arr4, arr5, arr6, arr7, arr8, arr9, arr10, arr11, arr12, arr13, arr14, arr15, arr16, arr17, arr18;
    arrow::Status st0, st1, st2, st3, st4, st5, st6, st7, st8, st9, st10, st11, st12, st13, st14, st15, st16, st17, st18;
    st0 = builder0.Finish(&arr0);
    st1 = builder1.Finish(&arr1);
    st2 = builder2.Finish(&arr2);
    st3 = builder3.Finish(&arr3);
    st4 = builder4.Finish(&arr4);
    st5 = builder5.Finish(&arr5);
    st6 = builder6.Finish(&arr6);
    st7 = builder7.Finish(&arr7);
    st8 = builder8.Finish(&arr8);
    st9 = builder9.Finish(&arr9);
    st10 = builder10.Finish(&arr10);
    st11 = builder11.Finish(&arr11);
    st12 = builder12.Finish(&arr12);
    st13 = builder13.Finish(&arr13);
    st14 = builder14.Finish(&arr14);
    st15 = builder15.Finish(&arr15);
    st16 = builder16.Finish(&arr16);
    st17 = builder17.Finish(&arr17);
    st18 = builder18.Finish(&arr18);
    std::vector<arrow::Status> status = {st0, st1, st2, st3, st4, st5, st6, st7, st8, st9, st10, st11, st12, st13, st14, st15, st16, st17, st18};
    for (const auto& s : status) {
        if (!s.ok()) {
            throw std::runtime_error(fmt::format("Error building arrow array from delta tuples"));
        }
    }

    std::vector<std::shared_ptr<arrow::Array>> arrowArrays = {arr0, arr1, arr2, arr3, arr4, arr5, arr6, arr7, arr8,
                                                              arr9, arr10, arr11, arr12, arr13, arr14, arr15, arr16, arr17, arr18};

    // TODO: check the schema is correct, now we just assume it is correct
    return TupleSet2::make(outputSchema, arrowArrays);
}