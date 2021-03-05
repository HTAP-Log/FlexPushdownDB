//
// Created by matt on 21/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H

#include <caf/all.hpp>
#include <normal/util/CAFUtil.h>
#include <normal/cache/SegmentCache.h>
#include <normal/core/cache/LoadRequestMessage.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/core/cache/StoreRequestMessage.h>
#include <normal/core/cache/WeightRequestMessage.h>
#include <normal/core/cache/CacheMetricsMessage.h>
#include <normal/cache/CachingPolicy.h>
#include <normal/core/Forward.h>

CAF_BEGIN_TYPE_ID_BLOCK(SegmentCacheActor, normal::util::SegmentCacheActor_first_custom_type_id)
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
// The following are explicit message types, so have to implement `inspect` for concrete derived shared_ptr type,
// implementing for base share_ptr type in this variant-based scheme doesn't work
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

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H
