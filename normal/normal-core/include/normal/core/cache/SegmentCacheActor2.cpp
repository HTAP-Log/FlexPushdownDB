//
// Created by matt on 21/8/20.
//

#include "SegmentCacheActor2.h"

using namespace normal::core::cache;

SegmentCacheActor2::behavior_type normal::core::cache::segmentCacheBehaviour(SegmentCacheActor2::stateful_pointer<SegmentCacheActor2State> self) {

  setDefaultHandlers(*self);

  return {
	  [=](StartAtom) {
		SPDLOG_DEBUG("Start");
	  },
	  [=](StopAtom) {
		SPDLOG_DEBUG("Stop");
	  },
	  [=](StoreAtom, int v) {
		SPDLOG_DEBUG("Store {}", v);
		return true;
	  },
	  [=](LoadAtom, int v) {
		SPDLOG_DEBUG("Load  |  actor id: '{}', actor name: '{}'", self->id(),
					 self->name());
		throw std::runtime_error("test exception");
	  }
  };
}
