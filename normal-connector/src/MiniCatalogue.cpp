//
// Created by Yifei Yang on 7/15/20.
//

#include <string>
#include <normal/connector/MiniCatalogue.h>
#include <normal/connector/s3/S3SelectPartition.h>
#include <fstream>
#include <utility>
#include <iostream>
#include <normal/cache/SegmentKey.h>
#include <arrow/api.h>
#include <fmt/format.h>
#include <filesystem>
#include <experimental/filesystem>

using namespace normal::cache;

namespace {

    std::shared_ptr<arrow::DataType> parseDataType(const std::string &s) {
        if (s == "int16" || s == "short") {
            return arrow::int16();
        } else if (s == "int32" || s == "int") {
            return arrow::int32();
        } else if (s == "int64" || s == "long") {
            return arrow::int64();
        } else if (s == "float32" || s == "float") {
            return arrow::float32();
        } else if (s == "float64" || s == "double") {
            return arrow::float64();
        } else if (s == "utf8" || s == "string") {
            return arrow::utf8();
        } else if (s == "boolean" || s == "bool") {
            return arrow::boolean();
        } else {
            throw std::runtime_error(fmt::format("Unrecognized data type string '{}'", s));
        }
    }

    char getCsvFileDelimiterForSchema(const std::string &schemaName) {
        // This is fine for now as only ssb-sf100-sortlineorder/csv_150MB/ uses a non comma delimiter among CSV files
        if (schemaName == "ssb-sf100-sortlineorder/csv_150MB/") {
            return '|';
        } else {
            return ',';
        }
    }

}
normal::connector::MiniCatalogue::MiniCatalogue(
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<::arrow::Schema>>> deltaSchemas,
        std::shared_ptr<std::unordered_map<std::string, std::string>> primaryKeys,
        std::string schemaName,
        std::shared_ptr<std::unordered_map<std::string, int>> partitionNums,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<::arrow::Schema>>> schemas,
        std::shared_ptr<std::unordered_map<std::string, int>> columnLengthMap,
        std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, size_t,
        SegmentKeyPointerHash, SegmentKeyPointerPredicate>> segmentKeyToSize,
        std::shared_ptr<std::unordered_map<int, std::shared_ptr<std::unordered_set
        <std::shared_ptr<cache::SegmentKey>, cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>>>> queryNumToInvolvedSegments,
        std::shared_ptr<std::unordered_map<std::shared_ptr<cache::SegmentKey>, std::shared_ptr<std::set<int>>,
        cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>> segmentKeysToInvolvedQueryNums,
        std::shared_ptr<std::vector<std::string>> defaultJoinOrder,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<
        std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>> sortedColumns,
        char csvFileDelimiter) :
        primaryKeys_(std::move(primaryKeys)),
        schemaName_(std::move(schemaName)),
        partitionNums_(std::move(partitionNums)),
        schemas_(std::move(schemas)),
        columnLengthMap_(std::move(columnLengthMap)),
        segmentKeyToSize_(std::move(segmentKeyToSize)),
        queryNumToInvolvedSegments_(std::move(queryNumToInvolvedSegments)),
        segmentKeysToInvolvedQueryNums_(std::move(segmentKeysToInvolvedQueryNums)),
        defaultJoinOrder_(std::move(defaultJoinOrder)),
        sortedColumns_(std::move(sortedColumns)),
        csvFileDelimiter_(csvFileDelimiter),
        deltaSchemas_(std::move(deltaSchemas)) {
    // initialize as 1, only needs to be updated for certain tasks (ie Belady Caching Policy)
    currentQueryNum_ = 1;
    // generate rowLengthMap from columnLengthMap
    rowLengthMap_ = std::make_shared<std::unordered_map<std::string, int>>();
    for (auto const &schemaEntry: *schemas_) {
        auto tableName = schemaEntry.first;
        int rowLength = 0;
        for (auto const &field: schemaEntry.second->fields()) {
            rowLength += columnLengthMap_->find(field->name())->second;
        }
        rowLengthMap_->emplace(tableName, rowLength);
    }
}

normal::connector::MiniCatalogue::MiniCatalogue(
        std::string schemaName,
        std::shared_ptr<std::unordered_map<std::string, int>> partitionNums,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<::arrow::Schema>>> schemas,
        std::shared_ptr<std::unordered_map<std::string, int>> columnLengthMap,
        std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, size_t,
                SegmentKeyPointerHash, SegmentKeyPointerPredicate>> segmentKeyToSize,
        std::shared_ptr<std::unordered_map<int, std::shared_ptr<std::unordered_set
                <std::shared_ptr<cache::SegmentKey>, cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>>>> queryNumToInvolvedSegments,
        std::shared_ptr<std::unordered_map<std::shared_ptr<cache::SegmentKey>, std::shared_ptr<std::set<int>>,
                cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>> segmentKeysToInvolvedQueryNums,
        std::shared_ptr<std::vector<std::string>> defaultJoinOrder,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<
                std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>> sortedColumns,
        char csvFileDelimiter) :
        schemaName_(std::move(schemaName)),
        partitionNums_(std::move(partitionNums)),
        schemas_(std::move(schemas)),
        columnLengthMap_(std::move(columnLengthMap)),
        segmentKeyToSize_(std::move(segmentKeyToSize)),
        queryNumToInvolvedSegments_(std::move(queryNumToInvolvedSegments)),
        segmentKeysToInvolvedQueryNums_(std::move(segmentKeysToInvolvedQueryNums)),
        defaultJoinOrder_(std::move(defaultJoinOrder)),
        sortedColumns_(std::move(sortedColumns)),
        csvFileDelimiter_(csvFileDelimiter) {

    // initialize as 1, only needs to be updated for certain tasks (ie Belady Caching Policy)
    currentQueryNum_ = 1;

    // generate rowLengthMap from columnLengthMap
    rowLengthMap_ = std::make_shared<std::unordered_map<std::string, int>>();
    for (auto const &schemaEntry: *schemas_) {
        auto tableName = schemaEntry.first;
        int rowLength = 0;
        for (auto const &field: schemaEntry.second->fields()) {
            rowLength += columnLengthMap_->find(field->name())->second;
        }
        rowLengthMap_->emplace(tableName, rowLength);
    }
}

std::vector<std::string> split(const std::string &str, const std::string &splitStr) {
    std::vector<std::string> res;
    std::string::size_type pos1, pos2;
    pos2 = str.find(splitStr);
    pos1 = 0;
    while (pos2 != std::string::npos) {
        res.push_back(str.substr(pos1, pos2 - pos1));
        pos1 = pos2 + 1;
        pos2 = str.find(splitStr, pos1);
    }
    if (pos1 < str.length()) {
        res.push_back(str.substr(pos1));
    }
    return res;
}

std::vector<std::string> readFileByLines(const std::filesystem::path &filePath) {
    std::ifstream file(filePath.string());
    std::vector<std::string> res;
    std::string str;
    while (getline(file, str)) {
        res.emplace_back(str);
    }
    return res;
}

std::shared_ptr<std::vector<std::pair<std::string, std::string>>>
readMetadataSort(const std::string &schemaName, const std::string &fileName) {
    auto res = std::make_shared<std::vector<std::pair<std::string, std::string>>>();
    auto filePath = std::filesystem::current_path().append("metadata").append(schemaName).append("sort").append(
            fileName);
    for (auto const &str: readFileByLines(filePath)) {
        auto splitRes = split(str, ",");
        res->emplace_back(std::pair<std::string, std::string>(splitRes[0], splitRes[1]));
    }
    return res;
}

std::shared_ptr<std::unordered_map<std::string, int>> readMetadataColumnLength(const std::string &schemaName) {
    auto res = std::make_shared<std::unordered_map<std::string, int>>();
    auto filePath = std::filesystem::current_path().append("metadata").append(schemaName).append("column_length");
    for (auto const &str: readFileByLines(filePath)) {
        auto splitRes = split(str, ",");
        res->emplace(splitRes[0], stoi(splitRes[1]));
    }
    return res;
}

std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<::arrow::Schema>>>
readMetadataSchemas(const std::string &schemaName) {
    auto res = std::make_shared<std::unordered_map<std::string, std::shared_ptr<::arrow::Schema>>>();
    auto filePath = std::filesystem::current_path().append("metadata").append(schemaName).append("schemas");
    for (auto const &str: readFileByLines(filePath)) {
        auto splitRes = split(str, ":");
        std::vector<std::shared_ptr<::arrow::Field>> fields;
        for (auto const &fieldEntry: split(splitRes[2], ",")) {
            auto splitFieldEntry = split(fieldEntry, "/");
            fields.push_back(::arrow::field(splitFieldEntry[0], parseDataType(splitFieldEntry[1])));
        }
        res->emplace(splitRes[0], ::arrow::schema(fields));
    }
    return res;
}

std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<::arrow::Schema>>>
readMetadataDeltaSchemas(const std::string &schemaName) {
    auto res = std::make_shared<std::unordered_map<std::string, std::shared_ptr<::arrow::Schema>>>();
    auto filePath = std::filesystem::current_path().append("metadata").append(schemaName).append("schemas");
    for (auto const &str: readFileByLines(filePath)) {
        auto splitRes = split(str, ":");
        std::vector<std::shared_ptr<::arrow::Field>> fields;
        for (auto const &fieldEntry: split(splitRes[2], ",")) {
            auto splitFieldEntry = split(fieldEntry, "/");
            fields.push_back(::arrow::field(splitFieldEntry[0], parseDataType(splitFieldEntry[1])));
        }
        fields.push_back(::arrow::field("timestamp", parseDataType("utf8")));
        fields.push_back(::arrow::field("type", parseDataType("utf8")));

        res->emplace(splitRes[0], ::arrow::schema(fields));
    }
    return res;
}

/**
 * Created by Han Cao Jun/4/2021.
 * Get the information of primary key column of each table from the metadata schema file.
 * @param schemaName name of the schema
 * @return a pointer to a map <tableName, primaryKeyColumnName>
 */
std::shared_ptr<std::unordered_map<std::string, std::string>> readPrimaryKeyName(const std::string &schemaName) {
    auto res = std::make_shared<std::unordered_map<std::string, std::string>>();
    auto filePath = std::experimental::filesystem::current_path().append("metadata").append(schemaName).append(
            "schemas");
    for (auto const &str: readFileByLines(static_cast<const std::filesystem::path>(filePath))) {
        auto splitRes = split(str, ":");
        res->emplace(splitRes[0], splitRes[1]);
    }

    return res;
}

std::shared_ptr<std::unordered_map<std::string, int>> readMetadataPartitionNums(const std::string &schemaName) {
    auto res = std::make_shared<std::unordered_map<std::string, int>>();
    auto filePath = std::filesystem::current_path().append("metadata").append(schemaName).append("partitionNums");
    for (auto const &str: readFileByLines(filePath)) {
        auto splitRes = split(str, ",");
        res->emplace(splitRes[0], stoi(splitRes[1]));
    }
    return res;
}

// Create a mapping of segmentKey -> size of segment with values already gathered
std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, size_t, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>
readMetadataSegmentInfo(const std::string &s3Bucket, const std::string &schemaName) {
    auto res = std::make_shared<std::unordered_map<std::shared_ptr<SegmentKey>, size_t, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>();
    auto filePath = std::filesystem::current_path().append("metadata").append(schemaName).append("segment_info");
    for (auto const &str: readFileByLines(filePath)) {
        auto splitRes = split(str, ",");
        std::string objectName = splitRes[0];
        std::string column = splitRes[1];
        unsigned long startOffset = std::stoul(splitRes[2]);
        unsigned long endOffset = std::stoul(splitRes[3]);
        size_t sizeInBytes;
        sscanf(splitRes[4].c_str(), "%zu", &sizeInBytes);

        std::string s3Object = schemaName + objectName;
        // create the SegmentKey
        auto segmentPartition = std::make_shared<S3SelectPartition>(s3Bucket, s3Object);
        auto segmentRange = SegmentRange::make(startOffset, endOffset);
        auto segmentKey = SegmentKey::make(segmentPartition, column, segmentRange);
        res->emplace(segmentKey, sizeInBytes);
    }
    return res;
}

std::shared_ptr<normal::connector::MiniCatalogue> normal::connector::MiniCatalogue::defaultMiniCatalogue(
        const std::string &s3Bucket, const std::string &schemaName) {
    // star join order
    auto defaultJoinOrder = std::make_shared<std::vector<std::string>>();
    defaultJoinOrder->emplace_back("supplier");
    defaultJoinOrder->emplace_back("date");
    defaultJoinOrder->emplace_back("customer");
    defaultJoinOrder->emplace_back("part");

    // schemas
    auto schemas = readMetadataSchemas(schemaName);

    // delta Schemas
    auto deltaSchemas = readMetadataDeltaSchemas(schemaName);

    // read the primary key
    auto primarykey = readPrimaryKeyName(schemaName);

    // partitionNums
    auto partitionNums = readMetadataPartitionNums(schemaName);

    // columnLengthMap
    auto columnLengthMap = readMetadataColumnLength(schemaName);

    // sortedColumns
    auto sortedColumns = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<
            std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>>();

    // sorted lo_orderdate
    auto sortedValues = std::make_shared<std::unordered_map<std::shared_ptr<Partition>, std::pair<std::string, std::string>,
            PartitionPointerHash, PartitionPointerPredicate>>();
    auto valuePairs = readMetadataSort(schemaName, "lineorder_orderdate");
    std::string s3ObjectDir = schemaName + "lineorder_sharded/";
    std::string fileExtension = normal::connector::getFileExtensionByDirPrefix(s3ObjectDir);
    for (int i = 0; i < (int) valuePairs->size(); i++) {
        sortedValues->emplace(std::make_shared<S3SelectPartition>(s3Bucket,
                                                                  s3ObjectDir + "lineorder" + fileExtension + "." +
                                                                  std::to_string(i)),
                              valuePairs->at(i));
    }
    sortedColumns->emplace("lo_orderdate", sortedValues);

    // initialize this as empty and populate it if necessary
    auto queryNumToInvolvedSegments = std::make_shared<std::unordered_map<int, std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>>>();

    // segmentKey to size
    auto segmentKeyToSize = readMetadataSegmentInfo(s3Bucket, schemaName);

    // initialize this as empty and populate it if necessary
    auto segmentKeysToInvolvedQueryNums = std::make_shared<std::unordered_map<std::shared_ptr<cache::SegmentKey>, std::shared_ptr<std::set<int>>,
            cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>>();

    char csvFileDelimiter = getCsvFileDelimiterForSchema(schemaName);
    return std::make_shared<MiniCatalogue>(deltaSchemas, primarykey, schemaName, partitionNums, schemas, columnLengthMap, segmentKeyToSize,
                                           queryNumToInvolvedSegments, segmentKeysToInvolvedQueryNums, defaultJoinOrder,
                                           sortedColumns, csvFileDelimiter);
}

const std::shared_ptr<std::unordered_map<std::string, int>> &normal::connector::MiniCatalogue::partitionNums() const {
    return partitionNums_;
}

const std::shared_ptr<std::vector<std::string>> &normal::connector::MiniCatalogue::defaultJoinOrder() const {
    return defaultJoinOrder_;
}

std::string normal::connector::MiniCatalogue::findTableOfColumn(const std::string &columnName) {
    for (const auto &schema: *schemas_) {
        for (const auto &field: schema.second->fields()) {
            if (field->name() == columnName) {
                return schema.first;
            }
        }
    }
    throw std::runtime_error("Column " + columnName + " not found");
}

std::shared_ptr<std::vector<std::string>>
normal::connector::MiniCatalogue::getColumnsOfTable(const std::string &tableName) {
    auto columns = std::make_shared<std::vector<std::string>>();
    for (const auto &schema: *schemas_) {
        if (schema.first == tableName) {
            for (const auto &field: schema.second->fields()) {
                columns->push_back(field->name());
            }
            return columns;
        }
    }
    throw std::runtime_error("table " + tableName + " not found");
}

size_t normal::connector::MiniCatalogue::getSegmentSize(const std::shared_ptr<cache::SegmentKey> &segmentKey) {
    auto key = segmentKeyToSize_->find(segmentKey);
    if (key != segmentKeyToSize_->end()) {
        return segmentKeyToSize_->at(segmentKey);
    }
    throw std::runtime_error("Segment key not found in getSegmentSize: " + segmentKey->toString());
}

// Used to populate the queryNumToInvolvedSegments_ and segmentKeysToInvolvedQueryNums_ mappings
void normal::connector::MiniCatalogue::addToSegmentQueryNumMappings(int queryNum,
                                                                    const std::shared_ptr<cache::SegmentKey> &segmentKey) {
    auto queryNumKeyEntry = queryNumToInvolvedSegments_->find(queryNum);
    if (queryNumKeyEntry != queryNumToInvolvedSegments_->end()) {
        queryNumKeyEntry->second->insert(segmentKey);
    } else {
        // first time seeing this queryNum, create a map entry for it
        auto segmentKeys = std::make_shared<std::unordered_set<std::shared_ptr<cache::SegmentKey>, cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>>();
        segmentKeys->insert(segmentKey);
        queryNumToInvolvedSegments_->emplace(queryNum, segmentKeys);
    }

    auto segmentKeyEntry = segmentKeysToInvolvedQueryNums_->find(segmentKey);
    if (segmentKeyEntry != segmentKeysToInvolvedQueryNums_->end()) {
        segmentKeyEntry->second->insert(queryNum);
    } else {
        // first time seeing this key, create a map entry for it
        auto queryNumbers = std::make_shared<std::set<int>>();
        queryNumbers->insert(queryNum);
        segmentKeysToInvolvedQueryNums_->emplace(segmentKey, queryNumbers);
    }
}

// Throws runtime exception if queryNum not present, which ensures failing fast in case of queryNum not being present
// If use cases of this method expand we can change this if necessary.
std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>
normal::connector::MiniCatalogue::getSegmentsInQuery(int queryNum) {
    auto key = queryNumToInvolvedSegments_->find(queryNum);
    if (key != queryNumToInvolvedSegments_->end()) {
        return key->second;
    }
    // we must not have populated this queryNum->[segments used] mapping for this queryNum,
    // throw an error as we should have never called this then
    throw std::runtime_error("Error, " + std::to_string(queryNum) +
                             " not populated in segmentKeysToInvolvedQueryNums_ in MiniCatalogue.cpp");
}

// Return the next query that this segmentKey is used in, if not used again according to the
// segmentKeysToInvolvedQueryNums_ mapping set via setSegmentKeysToInvolvedQueryNums then return -1
// Throws runtime exception if segmentKeysToInvolvedQueryNums_ is never set via setSegmentKeysToInvolvedQueryNums
int normal::connector::MiniCatalogue::querySegmentNextUsedIn(const std::shared_ptr<cache::SegmentKey> &segmentKey,
                                                             int currentQuery) {
    auto key = segmentKeysToInvolvedQueryNums_->find(segmentKey);
    if (key != segmentKeysToInvolvedQueryNums_->end()) {
        auto involvedQueriesList = key->second;
        auto nextQueryIt = involvedQueriesList->upper_bound(currentQuery);
        if (nextQueryIt != involvedQueriesList->end()) {
            return *nextQueryIt;
        }
        // TODO: segment never used again so return -1 to indicate this. Probably want a better strategy in the future
        return -1;
    }
    // must not exist in our queryNums, throw an error as we should have never called this then
    throw std::runtime_error(
            "Error, " + segmentKey->toString() + " next query requested but never should have been used");
}

double normal::connector::MiniCatalogue::lengthFraction(const std::string &columnName) {
    auto thisLength = columnLengthMap_->find(columnName)->second;
    auto tableName = findTableOfColumn(columnName);
    auto allLength = rowLengthMap_->find(tableName)->second;
    return (double) thisLength / (double) allLength;
}

const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_map<std::shared_ptr<Partition>, std::pair<std::string, std::string>, PartitionPointerHash, PartitionPointerPredicate>>>> &
normal::connector::MiniCatalogue::sortedColumns() const {
    return sortedColumns_;
}

std::shared_ptr<std::vector<std::string>> normal::connector::MiniCatalogue::tables() {
    auto tables = std::make_shared<std::vector<std::string>>();
    for (auto const &schema: *schemas_) {
        tables->emplace_back(schema.first);
    }
    return tables;
}

int normal::connector::MiniCatalogue::lengthOfRow(const std::string &tableName) {
    return rowLengthMap_->find(tableName)->second;
}

int normal::connector::MiniCatalogue::lengthOfColumn(const std::string &columnName) {
    return columnLengthMap_->find(columnName)->second;
}

void normal::connector::MiniCatalogue::setCurrentQueryNum(int queryNum) {
    currentQueryNum_ = queryNum;
}

int normal::connector::MiniCatalogue::getCurrentQueryNum() const {
    return currentQueryNum_;
}

std::shared_ptr<arrow::Schema> normal::connector::MiniCatalogue::getSchema(const std::string &tableName) {
    return schemas_->at(tableName);
}

char normal::connector::MiniCatalogue::getCSVFileDelimiter() const {
    return csvFileDelimiter_;
}

const std::string &normal::connector::MiniCatalogue::getSchemaName() const {
    return schemaName_;
}

std::string normal::connector::MiniCatalogue::getPrimaryKeyColumnName(const std::string &tableName) {
    return primaryKeys_->find(tableName)->second;
}

std::shared_ptr<arrow::Schema> normal::connector::MiniCatalogue::getDeltaSchema(const std::string &tableName) {
    return deltaSchemas_->at(tableName);
}

/**
 * Given table name and partition number, return how many delta files do we have for that partition
 * @param tableName name of the table
 * @param partitionNum
 * @return how many delta attached
 */
int normal::connector::MiniCatalogue::getNumberOfDeltas(const std::string &tableName,  const std::string &objectName) {
    // FIXME: Hardcode this for now
    if (tableName == "lineorder" && objectName == "super-small-ssb-htap/csv/stables/lineorder_sharded/lineorder.tbl.0") {
        return 1;
    }

    return 0;
}

std::string normal::connector::getFileExtensionByDirPrefix(const std::string &dir_prefix) {
    if (dir_prefix.find("csv") != std::string::npos) {
        if (dir_prefix.find("gzip") != std::string::npos) {
            return std::string(".gz.tbl");
        } else {
            return std::string(".tbl");
        }
    } else if (dir_prefix.find("parquet") != std::string::npos) {
        if (dir_prefix.find("snappy") != std::string::npos) {
            return std::string(".snappy.parquet");
        } else if (dir_prefix.find("gzip") != std::string::npos) {
            return std::string(".gzip.parquet");
        } else {
            return std::string(".parquet");
        }
    } else {
        // something went wrong, we either don't support this file type yet or an error occurred earlier
        throw std::runtime_error("Unknown file name to use for directory with prefix: " + dir_prefix);
    }
}