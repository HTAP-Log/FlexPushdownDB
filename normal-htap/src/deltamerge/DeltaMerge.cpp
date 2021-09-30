//
// Created by Han Cao on 7/15/21.
//

#include <deltamerge/DeltaMerge.h>
#include <normal/connector/MiniCatalogue.h>
#include <string>
#include <normal/tuple/ArrayAppender.h>
#include <normal/tuple/ArrayAppenderWrapper.h>
#include <normal/tuple/ColumnBuilder.h>

using namespace normal::htap::deltamerge;

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


std::shared_ptr <DeltaMerge> DeltaMerge::make(const std::string &tableName, const std::string &Name, long queryId,std::shared_ptr<::arrow::Schema> outputSchema ) {
    return std::make_shared<DeltaMerge>(tableName, Name, queryId, outputSchema);
}

/**
 * Called when recieve a message
 * @param msg
 */
void DeltaMerge::onReceive(const core::message::Envelope &msg) {
    if (msg.message().type() == "StartMessage") {
        this->onStart();
    } else if (msg.message().type() == "TupleMessage") {
        auto tupleMessage = dynamic_cast<const core::message::TupleMessage &>(msg.message());
        this->onTuple(tupleMessage);
    } else if (msg.message().type() == "CompleteMessage") {
        auto completeMessage = dynamic_cast<const core::message::CompleteMessage &>(msg.message());
        this->onComplete(completeMessage);

        // check if all producers complete sending the tuple, then we can start.
        if (allProducersComplete()) {
            deltaMerge();
        }
    } else {
        throw std::runtime_error("Unrecognized message type " + msg.message().type());
    }
}

/**
 * Called when the operator is start
 */
void DeltaMerge::onStart() {
    SPDLOG_DEBUG("Starting operator | name: '{}'", name());
}

/**
 * Check if all the producer operators finished transferring data
 * @return: true if all producers were finished, vice versa
 */
bool DeltaMerge::allProducersComplete() {
    return ctx()->operatorMap().allComplete(core::OperatorRelationshipType::Producer);
}

/**
 * When receive a tuple as message
 * @param message
 */
void DeltaMerge::onTuple(const core::message::TupleMessage &message) {
    const auto &tupleSet = TupleSet2::create(message.tuples()); // get all the useful rows

    if (deltaProducerNames_.count(message.sender())) {
        deltas_.emplace_back(tupleSet);
    } else if (stableProducerNames_.count(message.sender())) {
        stables_.emplace_back(tupleSet);
    } else {
        throw std::runtime_error(fmt::format("Unrecognized producer {}", message.sender()));
    }
}

/**
 * Notify that this actor is finished
 */
void DeltaMerge::onComplete(const core::message::CompleteMessage &) {
    if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(core::OperatorRelationshipType::Producer)) {
        ctx()->notifyComplete();
    }
}

void DeltaMerge::addStableProducer(const std::shared_ptr <Operator> &stableProducer) {
//    SPDLOG_CRITICAL(fmt::format("Delta Merge {}; Added {} as Producer", this->name(),stableProducer->name()));
    stableProducerNames_.insert(stableProducer->name());
    consume(stableProducer);
}

void DeltaMerge::addDeltaProducer(const std::shared_ptr <Operator> &deltaProducer) {
    deltaProducerNames_.insert(deltaProducer->name());
    consume(deltaProducer);
}

void DeltaMerge::populateArrowTrackers() {

    // right now assume all the tuples that we have are in arrow format
    auto miniCatalogue = normal::connector::defaultMiniCatalogue;
    deltaIndexTracker_ = std::vector(deltas_.size(), 0);
    stableIndexTracker_ = std::vector(stables_.size(), 0);

    // set up a process to obtain the needed columns (Primary Keys, Timestamp, Type)
    // FIXME: SUPPORT Composited PrimaryKey
//    std::vector<std::string> primaryKeys;
    std::string pk = miniCatalogue->getPrimaryKeyColumnName(tableName_);
//    primaryKeys.emplace_back(pk);

    // For deltas, we obtain three things: primary key, timestamp, type
    for (const auto& delta : deltas_) {
        std::vector<std::shared_ptr<Column>> columnTracker;
//        for (const auto& key : primaryKeys) {
//            auto keyColumn = delta->getColumnByName(key);
//            columnTracker.emplace_back(keyColumn.value());
//        }
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
//        for (const auto& key : primaryKeys) {
//            auto keyColumn = stable->getColumnByName(key);
//            columnTracker.emplace_back(keyColumn.value());
//        }

        auto pkColumn = stable->getColumnByName(pk);
        columnTracker.emplace_back(pkColumn.value());

        stableTracker_.emplace_back(columnTracker);
    }

}

/**
 * Determine which records to copy.
 */
void DeltaMerge::generateDeleteMaps() {

//    SPDLOG_CRITICAL(fmt::format("{}, IN generateDeleteMaps", this->name()));

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
        // now you get the smallest primary key
        // 1. Select all the deltas with the current primary key
        // 1.5 compare the timestamp and get one tuple only
        std::string minTS = "0";

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
            if (currPK != deltaTracker_[i][0]->element(deltaIndexTracker_[i]).value()->value<int32_t>()) continue;
            int tsIndex = deltaTracker_[0].size() - 2;
            auto currTS = deltaTracker_[i][tsIndex]->element(deltaIndexTracker_[i]).value()->toString();

            deltaIndexTracker_[i] += 1;

            if (currTS < minTS) {
                // update position
                minTS = currTS;
                position[0] = stableTracker_.size() + i;
                position[1] = deltaIndexTracker_[i];
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

//    SPDLOG_CRITICAL(fmt::format("{}, IN generateFinalResult", this->name()));

    // initialize an array of column appender
    std::vector<std::shared_ptr<ColumnBuilder>> columnBuilderArray(outputSchema_->num_fields());

    for (int i = 0; i < outputSchema_->num_fields(); i++) {
        std::string newColumnBuilderName = outputSchema_->field_names()[i] + "columnBuilder";
        auto newColumnBuilder = ColumnBuilder::make(newColumnBuilderName, outputSchema_->field(i)->type());
        columnBuilderArray[i] = newColumnBuilder;
    }

    // TODO: Change to column first
    // We first try to append the stable data to the new table
    for (int i = 0; i < stableTracker_.size(); i++) {
        auto deleteSet = deleteMap_.at(i); // get the deleteMap for this file
        auto originalTable  = stables_[i]; // Get the original stable file

        // now we try to loop through the entire file
        for (size_t r = 0; r < originalTable->numRows(); r++) {
            if (deleteSet.find(r) == deleteSet.end()) continue;
            // if the row is found, then copy it one column by one column
            for (size_t c = 0; c < outputSchema_->num_fields(); c++) {
                auto x = originalTable->getColumnByIndex(c).value()->element(r).value();
                columnBuilderArray[c]->append(x);
            }
        }
    }

    // Do the same thing again to the deltas
    for (int i = 0; i < deltaTracker_.size(); i++) {
        int offsetted_i = i + stableTracker_.size();
        auto deleteSet = deleteMap_.at(offsetted_i); // get the deleteMap for this file
        auto originalTable  = deltas_[i]; // Get the original stable file

        // now we try to loop through the entire file
        for (size_t r = 0; r < originalTable->numRows(); r++) {
            if (deleteSet.find(r) == deleteSet.end()) continue;
            // if the row is found, then copy it one column by one column
            for (size_t c = 0; c < outputSchema_->num_fields(); c++) {
                columnBuilderArray[c]->append(originalTable->getColumnByIndex(c).value()->element(r).value());
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

    SPDLOG_CRITICAL(fmt::format("{} populated called;", name()));

    generateDeleteMaps();

    auto output = generateFinalResult();

    std::shared_ptr<core::message::Message>
    tupleMessage = std::make_shared<core::message::TupleMessage>(output->toTupleSetV1(), name());
    ctx()->tell(tupleMessage);
}

DeltaMerge::DeltaMerge(std::string name, std::string type, long queryId1,
                       const std::__cxx11::basic_string<char> tableName, const std::basic_string<char> &Name,
                       long queryId) : Operator(name, type, queryId1) {

}
