//
// Created by Elena Milkai on 10/14/21.
//

#ifndef NORMAL_DELTACACHE_H
#define NORMAL_DELTACACHE_H

#include <unordered_map>
#include <memory>
#include <deltamanager/DeltaCacheKey.h>
#include <deltamanager/DeltaCacheData.h>
#include <normal/tuple/TupleSet2.h>

namespace normal::htap::deltamanager {
    class DeltaCache {
    public:
        explicit DeltaCache();
        static std::shared_ptr<DeltaCache> make();
        size_t getSize() const;
        /**
         * Function used to store to the input tail to the specific container in cache.
         * @param key
         * @param data
         */
        void store(const std::shared_ptr<DeltaCacheKey>& key, const std::shared_ptr<DeltaCacheData>& data);

        /**
         * Function used to return all the different versions of deltas for a specific table and partition.
         * @param key
         * @return the deltas and the timestamps in a vector of DeltaCacheData format.
         */
       std::vector<std::shared_ptr<DeltaCacheData>> load(const std::shared_ptr<DeltaCacheKey>& key);

        /**
         * Function used to evict deltas from cache when there is not enough space for the new tail  to be inserted.
         * @param key
         */
        void remove(const std::shared_ptr<DeltaCacheKey>& key);


    private:
        // a vector of containers (unordered_multimap), each one per table
        // choose multimap: each bucket can have multiple entries with same key, our key is the partition
        std::vector<std::unordered_multimap<std::shared_ptr<DeltaCacheKey>,
                                            std::shared_ptr<DeltaCacheData>,
                                            DeltaKeyPointerHash,
                                            DeltaKeyPointerPredicate>> deltaMap_;
    };

}
#endif //NORMAL_DELTACACHE_H