//
// Created by Han Cao on 7/15/21.
//

#include <deltamerge/DeltaMerge.h>
#include <normal/connector/MiniCatalogue.h>
#include <string>
#include <normal/tuple/ArrayAppender.h>
#include <normal/tuple/ArrayAppenderWrapper.h>
#include <normal/tuple/ColumnBuilder.h>
#include <deltamanager/DeltaCacheKey.h>
//#include "strtk.hpp"

using namespace normal::htap::deltamerge;

DeltaMerge::DeltaMerge(const std::string& tableName, const std::string &Name, long queryId, std::shared_ptr<::arrow::Schema> outputSchema, long partitionNumber) :
        Operator(Name, "deltamerge", queryId),
        outputSchema_(std::move(outputSchema)){
        tableName_ = tableName;
        partitionNumber_ = partitionNumber;
}

/**
 * Constructor
 * @param tableName: Name of the table
 * @param Name: Name of the operator
 * @param queryId
 */
DeltaMerge::DeltaMerge(const std::string& tableName, const std::string &Name, long queryId) :
        Operator(Name, "deltamerge", queryId) {
        tableName_ = tableName;
}
/**
 * Constructor
 * @param tableName
 * @param Name
 * @param queryId
 * @param outputSchema
 */
DeltaMerge::DeltaMerge(const std::string& tableName, const std::string &Name, long queryId, std::shared_ptr<::arrow::Schema> outputSchema) :
Operator(Name, "deltamerge", queryId),
outputSchema_(std::move(outputSchema)){
    tableName_ = tableName;
}

/**
 * Constructor
 * @param Name
 * @param queryId
 */
DeltaMerge::DeltaMerge(const std::string &Name, long queryId) :
        Operator(Name, "deltamerge", queryId) {
}

/**
 *
 * @param Name
 * @param queryId
 * @return
 */
std::shared_ptr <DeltaMerge> DeltaMerge::make(const std::string &Name, long queryId) {
    return std::make_shared<DeltaMerge>(Name, queryId);
}


std::shared_ptr <DeltaMerge> DeltaMerge::make(const std::string &tableName,
                                              const std::string &Name,
                                              long queryId,std::shared_ptr<::arrow::Schema> outputSchema ) {
    return std::make_shared<DeltaMerge>(tableName, Name, queryId, outputSchema);
}


std::shared_ptr <DeltaMerge> DeltaMerge::make(const std::string &tableName,
                                              const std::string &Name,
                                              long queryId, std::shared_ptr<::arrow::Schema> outputSchema,
                                              long partitionNumber) {
    return std::make_shared<DeltaMerge>(tableName, Name, queryId, outputSchema, partitionNumber);
}

/**
 * Called when recieve a message
 * @param msg
 */
void DeltaMerge::onReceive(const core::message::Envelope &msg) {
    if (msg.message().type() == "StartMessage") {
        this->onStart();
    } else if (msg.message().type() == "TupleMessage") {
        /*SPDLOG_CRITICAL("{}: Message of type {} was received from {}.", name(),
                        msg.message().type(), msg.message().sender());*/
        auto tupleMessage = dynamic_cast<const core::message::TupleMessage &>(msg.message());
        this->onTuple(tupleMessage);
    } else if (msg.message().type() == "LoadDeltasResponseMessage"){
        /*SPDLOG_CRITICAL("[7]. {}: Message of type {} was received from {}.", name(),
                        msg.message().type(), msg.message().sender());*/
        auto deltasMessage = dynamic_cast<const normal::htap::deltamanager::LoadDeltasResponseMessage &>(msg.message());
        this->onDeltas(deltasMessage);
    } else if (msg.message().type() == "CompleteMessage") {
        auto completeMessage = dynamic_cast<const core::message::CompleteMessage &>(msg.message());
        // check if all producers complete sending the tuple, then we can start.
        if (allProducersComplete()) {
            deltaMerge();
        }
        this->onComplete(completeMessage);
    } else {
        throw std::runtime_error("Unrecognized message type " + msg.message().type());
    }
}

/**
 * Called when the operator is start
 */
void DeltaMerge::onStart() {


    SPDLOG_INFO("Starting operator | name: '{}'", name());
    // send LoadDeltasRequestMessage to CacheHandler
    auto const &deltaKey  = deltamanager::DeltaCacheKey::make(this->tableName_, this->partitionNumber_);
    auto const &sender = name();
    std::string cacheHandlerName = "CacheHandler-"+this->tableName_+"-"+std::to_string(this->partitionNumber_);
    //SPDLOG_CRITICAL("{}, CacheHandler operator: {}", name(), cacheHandlerName);
    ctx()->send(deltamanager::LoadDeltasRequestMessage::make(deltaKey, sender),
                cacheHandlerName)
                .map_error([](auto err) { throw std::runtime_error(err); });
    //SPDLOG_CRITICAL("[1]. {}: Message of type LoadDeltasRequestMessage was send to {}.", name(), cacheHandlerName);
}

/**
 * Check if all the producer operators finished transferring data
 * @return: true if all producers were finished, vice versa
 */
bool DeltaMerge::allProducersComplete() {
    if (ctx()->operatorMap().allComplete(core::OperatorRelationshipType::Producer))  {
        wait_end = std::chrono::system_clock::now();
//        auto diff = time_end - time_start;
//        SPDLOG_CRITICAL(fmt::format("Operator: {}, duration: {}", name(), diff.count()));
    }
    return ctx()->operatorMap().allComplete(core::OperatorRelationshipType::Producer);
}

/**
 * When receive a tuple as message
 * @param message
 */
void DeltaMerge::onTuple(const core::message::TupleMessage &message) {
    const auto &tupleSet = TupleSet2::create(message.tuples()); // get all the useful rows

    if (deltas_.empty() && stables_.empty()) {
        wait_start = std::chrono::system_clock::now();
    }
    lastSenderName = message.sender();
    if (deltaProducerNames_.count(message.sender())) {
        deltas_.emplace_back(tupleSet);
    } else if (stableProducerNames_.count(message.sender())) {
        stables_.emplace_back(tupleSet);
    } else {
        throw std::runtime_error(fmt::format("Unrecognized producer {}", message.sender()));
    }
}

/**
 * Function responsible to handle the LoadDeltaResponseMessage with the in-memory deltas and their timestamps.
 * @param message of type LoadDeltasResponseMessage
 */
void DeltaMerge::onDeltas(const normal::htap::deltamanager::LoadDeltasResponseMessage &message){
    //SPDLOG_CRITICAL("OnDeltas is now handling the LoadDeltasResponseMessage.");
    const auto &deltas = message.getDeltas();
    const auto &timestamp = message.getTimestamps();
    memoryDeltas_.insert(memoryDeltas_.end(), deltas.begin(), deltas.end());
    memoryDeltaTimeStamp_.insert(memoryDeltaTimeStamp_.end(), timestamp.begin(), timestamp.end());
}

/**
 * Notify that this actor is finished
 */
void DeltaMerge::onComplete(const core::message::CompleteMessage &) {
    if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(core::OperatorRelationshipType::Producer)) {
        ctx()->notifyComplete();
       //  SPDLOG_CRITICAL("Notification was send for {}", name());
    }
}

void DeltaMerge::addStableProducer(const std::shared_ptr <Operator> &stableProducer) {
    // SPDLOG_CRITICAL(fmt::format("Delta Merge {}; Added {} as Producer", this->name(),stableProducer->name()));
    stableProducerNames_.insert(stableProducer->name());
    consume(stableProducer);
}

void DeltaMerge::addDeltaProducer(const std::shared_ptr <Operator> &deltaProducer) {
    deltaProducerNames_.insert(deltaProducer->name());
    consume(deltaProducer);
}

void DeltaMerge::addMemoryDeltaProducer(const std::shared_ptr<Operator> &memoryDeltaProducer) {
    memoryDeltaProducerNames_.insert(memoryDeltaProducer->name());
    consume(memoryDeltaProducer);
}

void DeltaMerge::populateArrowTrackers() {

    // right now assume all the tuples that we have are in arrow format
    auto miniCatalogue = normal::connector::defaultMiniCatalogue;
    deltaIndexTracker_ = std::vector(deltas_.size(), 0);
    stableIndexTracker_ = std::vector(stables_.size(), 0);
    memoryDeltaIndexTracker_ = std::vector(memoryDeltas_.size(), 0);

    // set up a process to obtain the needed columns (Primary Keys, Timestamp, Type)
    // FIXME: SUPPORT Composited PrimaryKey
    // std::vector<std::string> primaryKeys;
    std::string pk = miniCatalogue->getPrimaryKeyColumnName(tableName_);
    // primaryKeys.emplace_back(pk);

    for (const auto& delta : memoryDeltas_) {
        std::vector<std::shared_ptr<Column>> columnTracker;
        auto pkColumn = delta->getColumnByName(pk);
        auto typeColumn = delta->getColumnByName("type");
        //SPDLOG_CRITICAL("populateArrowTrackers: pkColumn: {}, typeColumn: {}", pkColumn.value()->toString(), typeColumn.value()->toString());
        columnTracker.emplace_back(pkColumn.value());
        columnTracker.emplace_back(typeColumn.value());
        memoryDeltaTracker_.emplace_back(columnTracker);
    }

    // For deltas, we obtain three things: primary key, timestamp, type
    for (const auto& delta : deltas_) {
        std::vector<std::shared_ptr<Column>> columnTracker;
        auto pkColumn = delta->getColumnByName(pk);
        auto timestampColumn = delta->getColumnByName("timestamp");
        auto typeColumn = delta->getColumnByName("type");
        columnTracker.emplace_back(pkColumn.value());
        columnTracker.emplace_back(timestampColumn.value());
        columnTracker.emplace_back(typeColumn.value());
        deltaTracker_.emplace_back(columnTracker);
    }

    // For stables, we're only obtaining the primary key
    for (const auto &stable :stables_) {
        std::vector<std::shared_ptr<Column>> columnTracker;
        auto pkColumn = stable->getColumnByName(pk);
        columnTracker.emplace_back(pkColumn.value());
        stableTracker_.emplace_back(columnTracker);
    }
}

/**
 * Determine which records to copy.
 */
void DeltaMerge::generateDeleteMaps() {

//     time_end = std::chrono::system_clock::now();
//     auto diff = time_end - time_start;
//     SPDLOG_CRITICAL(fmt::format("Operator: {}, duration: {}", name(), diff.count()));

    //SPDLOG_CRITICAL(fmt::format("{}, in generateDeleteMaps", this->name()));
    // FIXME: we do not consider composited primaryKey situation for now
    std::vector<int> stablePKStates;
    std::vector<int> deltaPKStates;

    while (!checkIfAllRecordsWereVisited()) {
        // loop through the stable and find the primary key
        int32_t currPK = std::numeric_limits<int32_t>::max();
        for (int i = 0; i < stableTracker_.size(); i++) {
            currPK = std::min(currPK, stableTracker_[i][0]->element(stableIndexTracker_[i]).value()->value<int32_t>());
        }

        for (int i = 0; i < deltaTracker_.size(); i++) {
            if (deltaIndexTracker_[i] >= deltaTracker_[i][0]->numRows()) {
                continue;
            }
            currPK = std::min(currPK, deltaTracker_[i][0]->element(deltaIndexTracker_[i]).value()->value<int32_t>());
        }

        for (int i = 0; i < memoryDeltaTracker_.size(); i++) {
            if (memoryDeltaIndexTracker_[i] >= memoryDeltaTracker_[i][0]->numRows()) {
                continue;
            }
            currPK = std::min(currPK, memoryDeltaTracker_[i][0]->element(memoryDeltaIndexTracker_[i]).value()->value<int32_t>());
        }
        // now you get the smallest primary key
        // 1. Select all the deltas with the current primary key
        // 1.5 compare the timestamp and get one tuple only

        std::string minTS_str = "0";
        int minTS = 0;

        std::array<int, 2> position = {0, 0}; // [deltaNum, idx]

        for (int i = 0; i < stableTracker_.size(); i++) {
            if (currPK == stableTracker_[i][0]->element(stableIndexTracker_[i]).value()->value<int32_t>()) {
                position[0] = i;
                position[1] = stableIndexTracker_[i];
                stableIndexTracker_[i] += 1;
            }
        }

        for (int i = 0; i < deltaTracker_.size(); i++) {
            if (deltaIndexTracker_[i] >= deltaTracker_[i][0]->numRows()) {
                continue;
            }
            if (currPK != deltaTracker_[i][0]->element(deltaIndexTracker_[i]).value()->value<int32_t>())
                continue;
            int tsIndex = deltaTracker_[0].size() - 2;
            auto currTS = deltaTracker_[i][tsIndex]->element(deltaIndexTracker_[i]).value()->toString();
            deltaIndexTracker_[i] += 1;
            if (currTS.compare(minTS_str)<0) {   //TODO: please find a more gentle solution
                // update position
                minTS_str = currTS;
                position[0] = stableTracker_.size() + i;
                position[1] = deltaIndexTracker_[i];
            }
        }

        for (int i = 0; i < memoryDeltaTracker_.size(); i++) {
            if (memoryDeltaIndexTracker_[i] >= memoryDeltaTracker_[i][0]->numRows())
                continue;

            if (currPK != memoryDeltaTracker_[i][0]->element(memoryDeltaIndexTracker_[i]).value()->value<int32_t>())
                continue;

            auto currTS = memoryDeltaTimeStamp_[i];

            memoryDeltaIndexTracker_[i] += 1;

            if (currTS < minTS) {
                // update position
                minTS = currTS;
                position[0] = stableTracker_.size() + deltaTracker_.size() + i;
                position[1] = memoryDeltaIndexTracker_[i];
            }

        }

        if (deleteMap_.find(position[0]) == deleteMap_.end()) {  // did not find the key
            std::unordered_set<int> temp = {position[1]};
            deleteMap_[position[0]] = temp;
        } else
            deleteMap_[position[0]].insert(position[1]);
        }
    }

/**
 * Based on the deleteMap, copy the needed values to a new TupleSet
 */
std::shared_ptr<TupleSet2> DeltaMerge::generateFinalResult() {

//    SPDLOG_CRITICAL(fmt::format("{}, in generateFinalResult", this->name()));

    // initialize an array of column appender
    std::vector<std::shared_ptr<ColumnBuilder>> columnBuilderArray(outputSchema_->num_fields());

    for (int i = 0; i < outputSchema_->num_fields(); i++) {
        std::string newColumnBuilderName = outputSchema_->field_names()[i] + "columnBuilder";
        auto newColumnBuilder = ColumnBuilder::make(newColumnBuilderName, outputSchema_->field(i)->type());
        columnBuilderArray[i] = newColumnBuilder;
    }

    // We first try to append the stable data to the new table
    for (int i = 0; i < stableTracker_.size(); i++) {
        std::unordered_set<int> deleteSet = {};
        if(deleteMap_.find(i) != deleteMap_.end())
            deleteSet = deleteMap_[i]; // get the deleteMap for this file
        auto originalTable  = stables_[i]; // Get the original stable file

        for (size_t c = 0; c < outputSchema_->num_fields(); c++) {
            auto curCol = originalTable->getColumnByIndex(c).value();
            for (size_t r = 0; r < originalTable->numRows(); r++) {
                if (deleteSet.count(r)) continue;
                auto x = curCol->element(r).value();
                columnBuilderArray[c]->append(x);
            }
        }
    }

    // Do the same thing again to the deltas
    for (int i = 0; i < deltaTracker_.size(); i++) {
        int offsetted_i = i + stableTracker_.size();
        std::unordered_set<int> deleteSet = {};
        if(deleteMap_.find(i) != deleteMap_.end())
            deleteSet = deleteMap_[offsetted_i]; // get the deleteMap for this file

        auto originalTable  = deltas_[i]; // Get the original stable file

        for (size_t c = 0; c < outputSchema_->num_fields(); c++) {
            auto curCol = originalTable->getColumnByIndex(c).value();
            for (size_t r = 0; r < originalTable->numRows(); r++) {
                if (deleteSet.find(r) == deleteSet.end()) continue;
                columnBuilderArray[c]->append(curCol->element(r).value());
            }
        }
    }

    // Do the same thing again to the memory deltas
    for (int i = 0; i < memoryDeltaTracker_.size(); i++) {
        int offsetted_i = i + stableTracker_.size() + deltaTracker_.size();
        std::unordered_set<int> deleteSet = {};
        if(deleteMap_.find(i) != deleteMap_.end())
            deleteSet = deleteMap_[offsetted_i]; // get the deleteMap for this file
        auto originalTable  = memoryDeltas_[i]; // Get the original stable file

        for (size_t c = 0; c < outputSchema_->num_fields(); c++) {
            auto curCol = originalTable->getColumnByIndex(c).value();
            for (size_t r = 0; r < originalTable->numRows(); r++) {
                if (deleteSet.find(r) == deleteSet.end()) continue;
                columnBuilderArray[c]->append(curCol->element(r).value());
            }
        }
    }


    std::vector<std::shared_ptr<Column>> builtColumns;
    // now we try to generate the final output
    for (auto & i : columnBuilderArray){
        builtColumns.emplace_back(i->finalize());
    }

    auto finalOutput = TupleSet2::make(normal::tuple::Schema::make(outputSchema_), builtColumns);
    
    return finalOutput;
}

/**
 *
 * @return true if not visited, false if visited all
 */
bool DeltaMerge::checkIfAllRecordsWereVisited() {
    for (int i = 0; i < deltaTracker_.size();i++) {
        if (deltaIndexTracker_[i] < deltaTracker_[i][0]->numRows() - 1) {
            return false;
        }

        if (stableIndexTracker_[i] < stableTracker_[i][0]->numRows() - 1) {
            return false;
        }
    }

    return true;
}

void DeltaMerge::deltaMerge() {
    populateArrowTrackers();

    //SPDLOG_CRITICAL(fmt::format("{} populated called.", name()));
    runtime_start = std::chrono::system_clock::now();

    auto start_1 = std::chrono::system_clock::now();
    generateDeleteMaps();
    auto end_1 = std::chrono::system_clock::now();

    //SPDLOG_CRITICAL(fmt::format("{} generateDeleteMaps called.", name()));

    auto start_2 = std::chrono::system_clock::now();
    auto output = generateFinalResult();
    auto end_2 = std::chrono::system_clock::now();

    runtime_end = std::chrono::system_clock::now();

    auto timeGenerateDeleteMap = end_1 - start_1;
    auto timeGenerateFinalResult = end_2 - start_2;

    // Display DeltaMerge Stats
    SPDLOG_INFO(fmt::format("Operator: {}, time generateDeleteMaps {}, time generateFinalResults {}", name(), timeGenerateDeleteMap.count(), timeGenerateFinalResult.count()));

    auto wait_time = wait_end - wait_start;
    auto runtime = runtime_end - runtime_start;
    logical_total_time = wait_time.count() + runtime.count();
    auto actual_temp = runtime_end - wait_start;
    actual_total_time = actual_temp.count();

    auto bench_msg = fmt::format("Operator: {}, wait time: {}, runtime: {}, logical total time: {}, actual total time: {}"
            , name(), wait_time.count(), runtime.count(), logical_total_time, actual_total_time);

    SPDLOG_INFO(bench_msg);
    SPDLOG_INFO(fmt::format("Operator: {}, last sender: {}", name(), lastSenderName));
    SPDLOG_INFO(fmt::format("{} generateFinalResult called.", name()));

    std::shared_ptr<core::message::Message>
    tupleMessage = std::make_shared<core::message::TupleMessage>(output->toTupleSetV1(), name());
    ctx()->tell(tupleMessage);

    //SPDLOG_CRITICAL("{}: ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~TupleMessage was send.", name());
}

DeltaMerge::DeltaMerge(std::string name, std::string type, long queryId1,
                       const std::__cxx11::basic_string<char> tableName, const std::basic_string<char> &Name,
                       long queryId) : Operator(name, type, queryId1) {

}
