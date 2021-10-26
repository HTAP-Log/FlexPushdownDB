//
// Created by Elena Milkai on 10/14/21.
//

#ifndef NORMAL_DELTACACHE_H
#define NORMAL_DELTACACHE_H

#include <unordered_map>
#include <memory>
#include <deltamanager/DeltaCacheKey.h>
#include <deltamanager/DeltaCacheData.h>

namespace normal::htap::deltamanager {
    class DeltaCache {
    public:
        explicit DeltaCache();
        static std::shared_ptr<DeltaCache> make();
        void store(const std::shared_ptr<DeltaCacheKey>& key, const std::shared_ptr<DeltaCacheData>& data);
        tl::expected<std::shared_ptr<DeltaCacheData>, std::string> load(const std::shared_ptr<DeltaCacheKey>& key);
        unsigned long remove(const std::shared_ptr<DeltaCacheKey>& key);
        std::shared_ptr<std::vector<std::shared_ptr<DeltaCacheKey>>> toCache(std::shared_ptr<std::vector<std::shared_ptr<DeltaCacheKey>>> deltaKeys);
        size_t getSize() const;
    private:
        // a vector of maps, each vector corresponds to a specific table and
        // partition and saves different versions of deltas
        std::unordered_map<std::shared_ptr<DeltaCacheKey>, std::shared_ptr<DeltaCacheData>, DeltaKeyPointerHash, DeltaKeyPointerPredicate> deltaMap_;
    };

}
#endif //NORMAL_DELTACACHE_H