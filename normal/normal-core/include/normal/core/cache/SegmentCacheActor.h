//
// Created by matt on 21/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H

#include <caf/all.hpp>

#include <normal/cache/SegmentCache.h>
#include <normal/core/cache/LoadRequestMessage.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/core/cache/StoreRequestMessage.h>
#include <normal/core/cache/WeightRequestMessage.h>
#include <normal/core/cache/CacheMetricsMessage.h>
#include <normal/cache/CachingPolicy.h>
#include <normal/core/Forward.h>
#include <normal/core/Globals.h>

CAF_BEGIN_TYPE_ID_BLOCK(SegmentCacheActor, normal::core::SegmentCacheActor_first_custom_type_id)
CAF_ADD_ATOM(SegmentCacheActor, LoadAtom)
CAF_ADD_ATOM(SegmentCacheActor, StoreAtom)
CAF_ADD_ATOM(SegmentCacheActor, WeightAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetNumHitsAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetNumMissesAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetNumShardHitsAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetNumShardMissesAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetCrtQueryNumHitsAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetCrtQueryNumMissesAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetCrtQueryNumShardHitsAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetCrtQueryNumShardMissesAtom)
CAF_ADD_ATOM(SegmentCacheActor, ClearMetricsAtom)
CAF_ADD_ATOM(SegmentCacheActor, ClearCrtQueryMetricsAtom)
CAF_ADD_ATOM(SegmentCacheActor, ClearCrtQueryShardMetricsAtom)
CAF_ADD_ATOM(SegmentCacheActor, MetricsAtom)
CAF_ADD_TYPE_ID(SegmentCacheActor, (std::shared_ptr<normal::core::cache::LoadResponseMessage>))
CAF_ADD_TYPE_ID(SegmentCacheActor, (std::shared_ptr<normal::core::cache::LoadRequestMessage>))
CAF_ADD_TYPE_ID(SegmentCacheActor, (std::shared_ptr<normal::core::cache::StoreRequestMessage>))
CAF_ADD_TYPE_ID(SegmentCacheActor, (std::shared_ptr<normal::core::cache::WeightRequestMessage>))
CAF_ADD_TYPE_ID(SegmentCacheActor, (std::shared_ptr<normal::core::cache::CacheMetricsMessage>))
CAF_END_TYPE_ID_BLOCK(SegmentCacheActor)

using namespace caf;
using namespace normal::cache;

namespace normal::core::cache {

struct SegmentCacheActorState {
  std::string name = "segment-cache";
  std::shared_ptr<SegmentCache> cache;
};

//using LoadAtom = atom_constant<atom("Load")>;
//using StoreAtom = atom_constant<atom("Store")>;
//using WeightAtom = atom_constant<atom("Weight")>;
//using GetNumHitsAtom = atom_constant<atom("NumHits")>;
//using GetNumMissesAtom = atom_constant<atom("NumMisses")>;
//using GetNumShardHitsAtom = atom_constant<atom("NumShrdHit")>;
//using GetNumShardMissesAtom = atom_constant<atom("NumShrdMis")>;
//using GetCrtQueryNumHitsAtom = atom_constant<atom("CNumHits")>;
//using GetCrtQueryNumMissesAtom = atom_constant<atom("CNumMisses")>;
//using GetCrtQueryNumShardHitsAtom = atom_constant<atom("CNmShrdHit")>;
//using GetCrtQueryNumShardMissesAtom = atom_constant<atom("CNmShrdMis")>;
//using ClearMetricsAtom = atom_constant<atom("ClrMetrics")>;
//using ClearCrtQueryMetricsAtom = atom_constant<atom("ClrCMetric")>;
//using ClearCrtQueryShardMetricsAtom = atom_constant<atom("ClrCShrMet")>;
//using MetricsAtom = atom_constant<atom("Metrics")>;

class SegmentCacheActor {

public:
  static behavior makeBehaviour(stateful_actor<SegmentCacheActorState> *self,
								const std::optional<std::shared_ptr<CachingPolicy>> &cachingPolicy);

  static std::shared_ptr<LoadResponseMessage> load(const LoadRequestMessage &msg,
												   stateful_actor<SegmentCacheActorState> *self);
  static void store(const StoreRequestMessage &msg, stateful_actor<SegmentCacheActorState> *self);
  static void weight(const WeightRequestMessage &msg, stateful_actor<SegmentCacheActorState> *self);
  static void metrics(const CacheMetricsMessage &msg, stateful_actor<SegmentCacheActorState> *self);

};

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::core::cache::LoadResponseMessage>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::core::cache::LoadRequestMessage>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::core::cache::StoreRequestMessage>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::core::cache::WeightRequestMessage>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::core::cache::CacheMetricsMessage>);

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H
