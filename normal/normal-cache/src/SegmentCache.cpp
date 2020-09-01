//
// Created by matt on 19/5/20.
//

#include "normal/cache/SegmentCache.h"

#include <utility>
#include <normal/cache/LRUCachingPolicy.h>

using namespace normal::cache;

SegmentCache::SegmentCache(std::shared_ptr<CachingPolicy> cachingPolicy) :
	cachingPolicy_(std::move(cachingPolicy)) {
}

std::shared_ptr<SegmentCache> SegmentCache::make() {
  return std::make_shared<SegmentCache>(LRUCachingPolicy::make());
}

std::shared_ptr<SegmentCache> SegmentCache::make(const std::shared_ptr<CachingPolicy>& cachingPolicy) {
  return std::make_shared<SegmentCache>(cachingPolicy);
}

void SegmentCache::store(const std::shared_ptr<SegmentKey> &key, const std::shared_ptr<SegmentData> &data) {
  auto removableKeys = cachingPolicy_->onStore(key);

  if (removableKeys.has_value()) {
    for (auto const &removableKey: *removableKeys.value()) {
      map_.erase(removableKey);
    }
    map_.emplace(key, data);
  }
}

tl::expected<std::shared_ptr<SegmentData>,
			 std::string> SegmentCache::load(const std::shared_ptr<SegmentKey> &key) {

  cachingPolicy_->onLoad(key);

  auto mapIterator = map_.find(key);
  if (mapIterator == map_.end()) {
    missNum_++;
	  return tl::unexpected(fmt::format("Segment for key '{}' not found", key->toString()));
  } else {
    hitNum_++;
    auto cacheEntry = mapIterator->second;
    return mapIterator->second;
  }
}

unsigned long SegmentCache::remove(const std::shared_ptr<SegmentKey> &key) {
  return map_.erase(key);
}

unsigned long SegmentCache::remove(const std::function<bool(const SegmentKey &)> &predicate) {
  unsigned long erasedCount = 0;
  for (const auto &mapEntry: map_) {
	if (predicate(*mapEntry.first)) {
	  cachingPolicy_->onRemove(mapEntry.first);
	  map_.erase(mapEntry.first);
	  ++erasedCount;
	}
  }
  return erasedCount;
}

size_t SegmentCache::getSize() const {
  return map_.size();
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
SegmentCache::toCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  return cachingPolicy_->onToCache(segmentKeys);
}

int SegmentCache::hitNum() const {
  return hitNum_;
}

int SegmentCache::missNum() const {
  return missNum_;
}

void SegmentCache::clearMetrics() {
  hitNum_ = 0;
  missNum_ = 0;
}
