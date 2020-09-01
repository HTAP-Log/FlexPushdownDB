//
// Created by matt on 4/6/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_CACHINGPOLICY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_CACHINGPOLICY_H

#include <memory>
#include <optional>
#include <vector>

#include "SegmentKey.h"

namespace normal::cache {

class CachingPolicy {

public:

  CachingPolicy(size_t maxSize);

  virtual ~CachingPolicy() = default;

  /**
   * Fired on load of an entry
   *
   * @param key
   */
  virtual void onLoad(const std::shared_ptr<SegmentKey> &key) = 0;

   /**
    * Fired on storage of an entry
    *
    * @param key
    * @return A vector of keys to remove from the cache, nullptr if segment cannot be stored
    */
  virtual std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> onStore(const std::shared_ptr<SegmentKey> &key) = 0;

  /**
   * Fired on removal of on entry
   *
   * @param key
   */
  virtual void onRemove(const std::shared_ptr<SegmentKey> &key) = 0;

  /**
   * Decide what segments to cache next
   *
   * @param keys of segments to access
   * @return keys of segments to cache next
   */
  virtual std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) = 0;

  /**
   * Show the current cache layout
   */
  virtual std::string showCurrentLayout() = 0;

protected:
  size_t maxSize_;
  size_t freeSize_;
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_CACHINGPOLICY_H
