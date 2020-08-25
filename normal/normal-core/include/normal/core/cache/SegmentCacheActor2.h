//
// Created by matt on 21/8/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR2_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR2_H
#include <caf/all.hpp>

#include <normal/cache/SegmentCache.h>
#include <normal/cache/LRUCachingPolicy.h>
#include <normal/core/Actors.h>

using namespace caf;
using namespace normal::cache;

namespace normal::core::cache {

using StartAtom = atom_constant<atom("start")>;
using StopAtom = atom_constant<atom("stop")>;
using StoreAtom = atom_constant<atom("store")>;
using LoadAtom = atom_constant<atom("load")>;
using ClearAtom = atom_constant<atom("clear")>;

using SegmentCacheActor2Actor = ::caf::typed_actor<reacts_to<StartAtom>,
												   reacts_to<StopAtom>,
												   replies_to<StoreAtom, int>::with<bool>,
												   reacts_to<LoadAtom, int>>;

struct SegmentCacheActor2State {
  std::string name = "segment-cache";
  std::unique_ptr<SegmentCache> cache = std::make_unique<SegmentCache>(LRUCachingPolicy::make());
};

SegmentCacheActor2Actor::behavior_type segmentCacheBehaviour(SegmentCacheActor2Actor::stateful_pointer <SegmentCacheActor2State> self);

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR2_H
