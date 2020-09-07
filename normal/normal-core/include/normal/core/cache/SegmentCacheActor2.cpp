//
// Created by matt on 21/8/20.
//

#include "SegmentCacheActor2.h"

#include <normal/core/Actors.h>

namespace {

using namespace normal::core;
using namespace normal::core::cache;

::caf::result<SegmentMap, std::vector<std::shared_ptr<SegmentKey>>>
onLoad(SegmentCacheActorType self, const std::vector<std::shared_ptr<SegmentKey>> &segmentKeys);

void onStore(SegmentCacheActorType self, const LegacySegmentMap &segments);

::caf::result<SegmentMap, std::vector<std::shared_ptr<SegmentKey>>>
onLoad(SegmentCacheActorType self, const std::vector<std::shared_ptr<SegmentKey>> &segmentKeys) {

  SPDLOG_DEBUG("Load  |  actor id: '{}', actor name: '{}'", self->id(), self->name());

  std::unordered_map<std::shared_ptr<SegmentKey>,
					 std::shared_ptr<SegmentData>,
					 SegmentKeyPointerHash,
					 SegmentKeyPointerPredicate> segments;
//  std::vector<std::shared_ptr<SegmentKey>> segmentKeysToCache;
  auto missSegmentKeys = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

//  SPDLOG_DEBUG("Handling load request. loadRequestMessage: {}", msg.toString());

  for (const auto &segmentKey: segmentKeys) {

	auto cacheData = self->state.cache->load(segmentKey);

	if (cacheData.has_value()) {
	  SPDLOG_DEBUG("Cache hit  |  segmentKey: {}", segmentKey->toString());
	  segments.insert(std::pair(segmentKey, cacheData.value()));

	} else {
	  SPDLOG_DEBUG("Cache miss  |  segmentKey: {}", segmentKey->toString());
	  missSegmentKeys->emplace_back(segmentKey);
	}
  }

  /*
   * Decision making should be locked FIXME: Is locking necessary?
   */
//  normal::cache::replacementGlobalLock.lock();
  auto segmentKeysToCache = self->state.cache->toCache(missSegmentKeys);
//  normal::cache::replacementGlobalLock.unlock();

  return make_result(segments, *segmentKeysToCache);

//  auto responseMessage = LoadResponseMessage::make(segments, name(), *segmentKeysToCache);
//
//  ctx()->send(responseMessage, msg.sender())
//	  .map_error([](auto err) { throw std::runtime_error(fmt::format("{}, SegmentCacheActor", err)); });
}

void onStore(SegmentCacheActorType self, const LegacySegmentMap &segments) {
//  SPDLOG_DEBUG("Store  |  storeMessage: {}", msg.toString());
  for(const auto &segmentEntry: segments){
	auto segmentKey = segmentEntry.first;
	auto segmentData = segmentEntry.second;
	self->state.cache->store(segmentKey, segmentData);
  }
}

}

namespace normal::core::cache {

SegmentCacheActor2::behavior_type segmentCacheBehaviour(SegmentCacheActorType self) {

  SPDLOG_DEBUG("[Actor {} ('{}')] Segment Cache Actor Spawn", self->id(), self->name());

  setDefaultHandlers(*self);

  return {
	  [=](StartAtom) {
		SPDLOG_DEBUG("Start");
	  },
	  [=](StopAtom) {
		SPDLOG_DEBUG("Stop");
	  },
	  [=](StoreAtom, const LegacySegmentMap& segments) {
		return onStore(self, segments);
	  },
	  [=](LoadAtom, const std::vector<std::shared_ptr<SegmentKey>> &segmentKeys) {
		return onLoad(self, segmentKeys);
	  }
  };
}

}