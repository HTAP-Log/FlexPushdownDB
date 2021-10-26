//
// Created by Elena Milkai on 10/14/21.
//

#include <deltamanager/CacheTail.h>

using namespace normal::htap::deltamanager;

CacheTail::CacheTail(const std::string& OperatorName, const std::string& tableName,  const int &partition,
                     const int &timestamp,  const long queryId):
                     core::Operator(OperatorName, "CacheTail", queryId){
    tableName_ = tableName;
    partition_ = partition;
    timestamp_ = timestamp;
}

std::shared_ptr<GetTailDeltas> GetTailDeltas::make(const std::string& OperatorName, const std::string& tableName,
                                                   const int &partition, const int &timestamp,  const long queryId) {
    return std::make_shared<CacheTail>(OperatorName, tableName, partition, timestamp, queryId);
}

void GetTailDeltas::onReceive(const core::message::Envelope &msg) {
    // This actor simply starts
    if (msg.message().type() == "StartMessage") {
        this->onStart();
    } else {
        throw std::runtime_error(fmt::format("Unrecognized message type: {}, {}", msg.message().type(), name()));
    }
}

void GetTailDeltas::onStart() {
    SPDLOG_DEBUG("Starting operator '{}'", name());
    // TODO: check if there should be a conditional check
    GetTailDeltas::readAndSendDeltas(tableName_, partition_);
}

void GetTailDeltas::readAndSendDeltas(const std::string& tableName, const int partition, const int timestamp) {
    std::shared_ptr<TupleSet2> deltaPartitionTable;
    if (tableName.empty()) {
        deltaPartitionTable = TupleSet2::make();
    } else {
        SPDLOG_DEBUG("Getting table '{}' with partition '{}' from binlog at timestamp '{}'...", tableName, partition, timestamp);

        // now call binlog parser API
        std::unordered_map<int, std::set<struct lineorder_record>> *lineorder_record_ptr = nullptr;
        // TODO: change this hardcoded method
        const char* path = "./bin.000002"; //binlog file path
        const char* path_range = "./partitions/ranges.csv"; //range file path
        BinlogParser binlogParser;
        binlogParser.parse(path, path_range, &lineorder_record_ptr);
        if (lineorder_record_ptr == nullptr) {
            throw std::runtime_error(fmt::format("Error parsing binlog; table '{}', partition '{}'", tableName, partition));
        }
        if (lineorder_record_ptr->find(partition) == lineorder_record_ptr->end()) {
            throw std::runtime_error(fmt::format("No partition '{}' exists in table '{}'", partition, tableName));
        }
        std::set<struct lineorder_record> deltaRecords = (*lineorder_record_ptr)[partition];
        // put all the tuple into rowDelta vector
        std::vector<LineorderDelta_t> deltaTuples(deltaRecords.size());
        for (const auto& record : deltaRecords) {
            deltaTuples.emplace_back(record.lineorder_delta);
        }
        deltaPartitionTable = GetTailDeltas::rowToColumn(deltaTuples);
    }
    // construct actor message for DeltaMerge
    std::shared_ptr<normal::core::message::Message>
            message = std::make_shared<core::message::TupleMessage>(deltaPartitionTable->toTupleSetV1(), this->name());

    ctx()->tell(message);
    ctx()->notifyComplete();
}

