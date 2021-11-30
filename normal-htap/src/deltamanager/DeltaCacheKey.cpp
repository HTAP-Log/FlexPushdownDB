//
// Created by Elena Milkai on 10/11/21.
//

#include <deltamanager/DeltaCacheKey.h>
using namespace normal::htap::deltamanager;

DeltaCacheKey::DeltaCacheKey(const std::string& tableName,
                             const int& partition){
    tableName_ = tableName;
    partition_ = partition;
}

std::shared_ptr<DeltaCacheKey> DeltaCacheKey::make(const std::string& tableName,
                                                   const int& partition){
    return std::make_shared<DeltaCacheKey>(tableName,
                                           partition);
}

const std::string &DeltaCacheKey::getTableName() const {
    return tableName_;
}

const int &DeltaCacheKey::getPartition() const{
    return partition_;
}

size_t DeltaCacheKey::hash(){
    return std::hash<int>()(partition_);
    //return std::hash<std::string>()(tableName_) ^ (std::hash<int>()(partition_) << 2);
}

int DeltaCacheKey::tableToVector(){
    int idx = -1;
    if (tableName_ == "lineorder")
        idx = 0;
    else if (tableName_ == "customer")
        idx = 1;
    else if (tableName_ == "parts")
        idx = 2;
    else if (tableName_ == "date")
        idx = 3;
    else
        idx = 4;
    return idx;
}

bool DeltaCacheKey::operator==(const DeltaCacheKey &other) const {
    return partition_ == other.partition_;
}

bool DeltaCacheKey::operator!=(const DeltaCacheKey &other) const {
    return !(*this == other);
}

std::string DeltaCacheKey::toString() {
    return fmt::format("{{ tableName: {}, partition: {} }}", tableName_, std::to_string(partition_));
}
