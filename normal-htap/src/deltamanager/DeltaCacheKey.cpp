//
// Created by Elena Milkai on 10/11/21.
//

#include <deltamanager/DeltaCacheKey.h>
using namespace normal::htap::deltamanager;

DeltaCacheKey::DeltaCacheKey(const std::string& tableName,
                             const int& partition,
                             const int& timestamp){
    tableName_ = tableName;
    partition_ = partition;
    timestamp_ = timestamp;
}

std::shared_ptr<DeltaCacheKey> DeltaCacheKey::make(const std::string& tableName,
                                                   const int& partition,
                                                   const int& timestamp){
    return std::make_shared<DeltaCacheKey>(tableName,
                                           partition,
                                           timestamp);
}

const std::string &DeltaCacheKey::getTableName() const {
    return tableName_;
}

const int &DeltaCacheKey::getPartition() const{
    return partition_;
}

const int &DeltaCacheKey::getTimestamp() const {
    return timestamp_;
}

size_t DeltaCacheKey::hash(){
    return std::hash<std::string>()(tableName_) ^ (std::hash<int>()(partition_) << 2);
}

bool DeltaCacheKey::operator==(const DeltaCacheKey &other) const {
    return tableName_ == other.tableName_ &&
           partition_ == other.partition_;
}

bool DeltaCacheKey::operator!=(const DeltaCacheKey &other) const {
    return !(*this == other);
}

std::string DeltaCacheKey::toString() {
    return fmt::format("{{ tableName: {}, partition: {}, timestamp: {} }}", tableName_, std::to_string(partition_), std::to_string(timestamp_));
}
