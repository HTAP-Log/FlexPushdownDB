//
// Created by Elena Milkai on 10/14/21.
//

#include <deltamanager/DeltaCache.h>

using namespace normal::htap::deltamanager;

DeltaCache::DeltaCache(){}

std::shared_ptr<DeltaCache> DeltaCache::make(){
    return std::make_shared<DeltaCache>();
}

void DeltaCache::store(const std::shared_ptr<DeltaCacheKey>& key, const std::shared_ptr<DeltaCacheData>& data){
    deltaMap_.emplace(key, data);
}

unsigned long DeltaCache::remove(const std::shared_ptr<DeltaCacheKey>& key){
    for(unsigned i=0; i<deltaMap_.bucket_count(); i++) {
        for (auto it = deltaMap_.begin(i); it != deltaMap_.end(i); it++) {
            /*if (it->first == key->getTableName() && it->second == key->getPartition() && it->third == key->getTimestamp())
                deltaMap_.erase(i);
            else
                break;*/
        }
    }
}