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

size_t DeltaCache::getSize() const {
    return deltaMap_[0].size();
}

void DeltaCache::store(const std::shared_ptr<DeltaCacheKey>& key, const std::shared_ptr<DeltaCacheData>& data){
    deltaMap_[key->tableToVector()].emplace(key, data);
}

std::vector<std::shared_ptr<DeltaCacheData>> DeltaCache::load(const std::shared_ptr<DeltaCacheKey>& key){
    std::vector<std::shared_ptr<DeltaCacheData>> timestampedDeltas(0);
    int idx = key->tableToVector();
    for ( auto it = deltaMap_[idx].begin(); it != deltaMap_[idx].end(); ++it){
        std::shared_ptr<DeltaCacheData> deltaCacheEntry =
                std::make_shared<DeltaCacheData>(it->second->getDelta(), it->second->getTimestamp());
        timestampedDeltas.emplace_back(deltaCacheEntry);
    }
    return timestampedDeltas;
}

void DeltaCache::remove(const std::shared_ptr<DeltaCacheKey>& key){
    int idx = key->tableToVector();
    deltaMap_[idx].erase(key);
}
