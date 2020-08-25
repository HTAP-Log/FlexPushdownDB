//
// Created by matt on 21/8/20.
//

#include <doctest/doctest.h>
#include <caf/all.hpp>

#include <normal/core/SystemActor.h>
#include <normal/core/cache/SegmentCacheActor2.h>
#include <normal/core/QueryActor.h>
#include <normal/core/Actors.h>
#include <normal/core/Normal.h>

using namespace caf;
using namespace normal::core;
using namespace normal::core::cache;

TEST_SUITE ("actor") {

TEST_CASE ("actor-spawn2" * doctest::skip(false)) {

//  ::caf::actor_system_config actorSystemConfig;
//
//  auto actorSystem = std::make_unique<::caf::actor_system>(actorSystemConfig);
//
//  auto clientActor = std::make_unique<caf::scoped_actor>(*actorSystem);
//
//  auto systemActorHandle = actorSystem->spawn(systemBehaviour);
//
//  (*clientActor)->send(systemActorHandle, StartAtom::value);
//
//  auto segmentCacheActorHandle = actorSystem->spawn(segmentCacheBehaviour);
//
////  (*clientActor)->send(systemActorHandle, MonitorAtom::value, actor_cast<actor>(segmentCacheActorHandle));
//
//  (*clientActor)->send(segmentCacheActorHandle, StartAtom::value);
//
//  auto query01ActorHandle = actorSystem->spawn(queryBehaviour, "query-01");
//  (*clientActor)->send(query01ActorHandle, StartAtom::value);
//
//  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
//
//  (*clientActor)->send(segmentCacheActorHandle, StoreAtom::value, 100);
//  (*clientActor)->request(segmentCacheActorHandle, ::caf::infinite, StoreAtom::value, 100)
//	  .receive([&](bool x) {
//				 SPDLOG_DEBUG("Store Response {}", x);
//			   },
//			   [&](const error &err) {
//				 SPDLOG_ERROR("Store error {}", err.context().stringify(0));
//	  });
//  (*clientActor)->send(segmentCacheActorHandle, LoadAtom::value, 100);
//
//  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
//
//  (*clientActor)->send(segmentCacheActorHandle, StopAtom::value);
//  (*clientActor)->send(systemActorHandle, StopAtom::value);
//
//  (*clientActor)->send_exit(query01ActorHandle, ::caf::exit_reason::user_shutdown);
//  (*clientActor)->send_exit(segmentCacheActorHandle, ::caf::exit_reason::user_shutdown);
//  (*clientActor)->send_exit(systemActorHandle, ::caf::exit_reason::user_shutdown);
//
//  clientActor.reset();
//
//  actorSystem->await_all_actors_done();
}

TEST_CASE ("normal-lifecycle" * doctest::skip(false)) {
  auto n = Normal::start();

  std::shared_ptr<OperatorGraph> q;
  auto r = n->execute(q);

  SPDLOG_DEBUG("Result:\n{}", r.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  n->stop();
}

}
