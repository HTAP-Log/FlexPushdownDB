//
// Created by Elena Milkai on 10/14/21.
//

#include <deltamanager/DeltaCache.h>

using namespace normal::htap::deltamanager;

DeltaCache::DeltaCache(){
    // number of vectors hardcoded (equal to number of tables), need to get from metadata
    deltaMap_ = std::vector<std::unordered_multimap<std::shared_ptr<DeltaCacheKey>,
                                               std::shared_ptr<DeltaCacheData>,
                                               DeltaKeyPointerHash,
                                               DeltaKeyPointerPredicate>>(5);
}

std::shared_ptr<DeltaCache> DeltaCache::make(){
    return std::make_shared<DeltaCache>();
}

void DeltaCache::store(const std::shared_ptr<DeltaCacheKey>& key, const std::shared_ptr<DeltaCacheData>& data){
    deltaMap_[key->tableToVector()].emplace(key, data);
}
std::shared_ptr<TupleSet2> DeltaCache::load(const std::shared_ptr<DeltaCacheKey>& key){
    std::shared_ptr<TupleSet2> deltaTuples = nullptr;
    int idx = key->tableToVector();
    int bucket = key->hash();
    for (auto it=deltaMap_[idx].begin(bucket); it!=deltaMap_[idx].end(bucket); it++){
        if (deltaTuples)
            deltaTuples = TupleSet2::concatenate({it->second->getDelta(), deltaTuples}).value();
        else
            deltaTuples = it->second->getDelta();
    }
    return deltaTuples;
}

void DeltaCache::remove(const std::shared_ptr<DeltaCacheKey>& key){
    int idx = key->tableToVector();
    deltaMap_[idx].erase(key);
}