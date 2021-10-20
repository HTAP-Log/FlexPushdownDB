//
// Created by Elena Milkai on 10/14/21.
//

#include <deltamanager/DeltaCache.h>

using namespace normal::htap::deltamanager;

DeltaCache::DeltaCache(){}

static std::shared_ptr<DeltaCache> DeltaCache::make(){
    return std::make_shared<DeltaCache>();
}

void DeltaCache::store(const std::shared_ptr<DeltaKey>& key, const std::shared_ptr<DeltaData>& data){
    deltaMap_.emplace(key, data);
}

unsigned long DeltaCache::remove(const std::shared_ptr<DeltaKey>& key){
    for(unsigned i=0; i<deltaMap_.bucket_count(); i++) {
        for (auto it = deltamap_.begin(i); it != deltamap_.end(i); it++) {
            if (it->first == key->getTableName() && it->second == key->getPartition() &&
                it->third == key->getTimestamp())
                deltamap_.erase(it);
            else
                break;
        }
    }
}