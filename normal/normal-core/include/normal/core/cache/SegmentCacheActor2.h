//
// Created by matt on 21/8/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR2_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR2_H

#include <caf/all.hpp>

#include <normal/cache/SegmentCache.h>
#include <normal/cache/LRUCachingPolicy.h>
#include <normal/core/Actors.h>
#include <normal/core/cache/SegmentCacheActor.h>

using namespace caf;
using namespace normal::cache;

namespace normal::core::cache {

using StartAtom = atom_constant<atom("start")>;
using StopAtom = atom_constant<atom("stop")>;
using StoreAtom = atom_constant<atom("store")>;
using LoadAtom = atom_constant<atom("load")>;
using ClearAtom = atom_constant<atom("clear")>;

using SegmentMap = std::unordered_map<std::shared_ptr<SegmentKey>,
									  std::shared_ptr<SegmentData>,
									  SegmentKeyPointerHash,
									  SegmentKeyPointerPredicate>;

using LegacySegmentMap = std::unordered_map<std::shared_ptr<SegmentKey>,
									  std::shared_ptr<SegmentData>>;

using SegmentCacheActor2 = ::caf::typed_actor<reacts_to<StartAtom>,
											  reacts_to<StopAtom>,
											  reacts_to<StoreAtom, LegacySegmentMap>,
											  replies_to<LoadAtom, std::vector<std::shared_ptr<SegmentKey>>>
											  ::with<SegmentMap, std::vector<std::shared_ptr<SegmentKey>>>>;

struct SegmentCacheActor2State {
  std::string name = "segment-cache";
  std::unique_ptr<SegmentCache> cache = std::make_unique<SegmentCache>(LRUCachingPolicy::make());
};

using SegmentCacheActorType = SegmentCacheActor2::stateful_pointer<SegmentCacheActor2State>;

SegmentCacheActor2::behavior_type segmentCacheBehaviour(SegmentCacheActorType self);

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::vector<std::shared_ptr<SegmentKey>>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(normal::core::cache::SegmentMap);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(normal::core::cache::LegacySegmentMap);

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR2_H
