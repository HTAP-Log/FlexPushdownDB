//
// Created by Elena Milkai on 10/14/21.
//

#ifndef NORMAL_DELTACACHE_H
#define NORMAL_DELTACACHE_H

#include <unordered_map>
#include <memory>
#include "DeltaCacheKey.h"
#include "DeltaCacheData.h"

namespace normal::htap::deltamanager {
    class DeltaCache {
    public:
        explicit DeltaCache();
        static std::shared_ptr<DeltaCache> make();
        void store(const std::shared_ptr<DeltaKey>& key, const std::shared_ptr<DeltaData>& data);
        tl::expected<std::shared_ptr<DeltaData>, std::string> load(const std::shared_ptr<DeltaKey>& key);
        unsigned long remove(const std::shared_ptr<DeltaKey>& key);
        std::shared_ptr<std::vector<std::shared_ptr<DeltaKey>>> toCache(std::shared_ptr<std::vector<std::shared_ptr<DeltaKey>>> deltaKeys);
        size_t getSize() const;
    private:
        // a vector of maps, each vector corresponds to a specific table and
        // partition and saves different versions of deltas
        std::unordered_map<std::shared_ptr<DeltaKey>, std::shared_ptr<DeltaData>, DeltaKeyPointerHash, DeltaKeyPointerPredicate> deltaMap_;
    };

}
#endif //NORMAL_DELTACACHE_H