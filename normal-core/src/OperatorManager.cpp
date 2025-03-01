//
// Created by matt on 5/12/19.
//

#include <normal/core/OperatorManager.h>
#include <normal/core/cache/SegmentCacheActor.h>
#include <normal/core/message/Envelope.h>
#include <deltamanager/DeltaCacheActor.h>

using namespace normal::core::cache;
using namespace normal::core::message;
using namespace normal::htap::deltamanager;

namespace normal::core {

void OperatorManager::start() {
  running_ = true;
}

void OperatorManager::stop() {

  // Send actors a shutdown message
  (*rootActor_)->send_exit(caf::actor_cast<caf::actor>(segmentCacheActor_), caf::exit_reason::user_shutdown);
  (*rootActor_)->send_exit(caf::actor_cast<caf::actor>(deltaCacheActor_), caf::exit_reason::user_shutdown);

  // Stop the root actor (seems, being defined by "scope", it needs to actually be destroyed to stop it)
  rootActor_.reset();

  this->actorSystem->await_all_actors_done();

  running_ = false;
}

OperatorManager::OperatorManager() : queryCounter_(0), running_(false){
  actorSystem = std::make_unique<caf::actor_system>(actorSystemConfig);
  rootActor_ = std::make_shared<caf::scoped_actor>(*actorSystem);
}

OperatorManager::OperatorManager(std::shared_ptr<CachingPolicy>  cachingPolicy) :
  cachingPolicy_(std::move(cachingPolicy)),
  queryCounter_(0),
  running_(false){
  actorSystem = std::make_unique<caf::actor_system>(actorSystemConfig);
  rootActor_ = std::make_shared<caf::scoped_actor>(*actorSystem);
}

void OperatorManager::boot() {
  if (cachingPolicy_) {
  	segmentCacheActor_ = actorSystem->spawn(SegmentCacheActor::makeBehaviour, cachingPolicy_);
    deltaCacheActor_ = actorSystem->spawn(DeltaCacheActor::makeBehaviour);
  } else {
    segmentCacheActor_ = actorSystem->spawn(SegmentCacheActor::makeBehaviour, std::nullopt);
    deltaCacheActor_ = actorSystem->spawn(DeltaCacheActor::makeBehaviour);
  }
}

std::string OperatorManager::showCacheMetrics() {
  int hitNum, missNum;
  int shardHitNum, shardMissNum;

  auto errorHandler = [&](const caf::error& error){
	throw std::runtime_error(to_string(error));
  };

  scoped_actor self{*actorSystem};
  self->request(segmentCacheActor_, infinite, GetNumHitsAtom::value).receive(
	  [&](int numHits) {
		hitNum = numHits;
	  },
	  errorHandler);

  self->request(segmentCacheActor_, infinite, GetNumMissesAtom::value).receive(
	  [&](int numMisses) {
		missNum = numMisses;
	  },
	  errorHandler);

  double hitRate = (hitNum + missNum == 0) ? 0.0 : (double) hitNum / (double) (hitNum + missNum);

  self->request(segmentCacheActor_, infinite, GetNumShardHitsAtom::value).receive(
	  [&](int numShardHits) {
		shardHitNum = numShardHits;
	  },
	  errorHandler);

  self->request(segmentCacheActor_, infinite, GetNumShardMissesAtom::value).receive(
	  [&](int numShardMisses) {
		shardMissNum = numShardMisses;
	  },
	  errorHandler);

  double shardHitRate = (shardHitNum + shardMissNum == 0) ? 0.0 : (double) shardHitNum / (double) (shardHitNum + shardMissNum);

  std::stringstream ss;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Hit num:";
  ss << std::left << std::setw(40) << hitNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Miss num:";
  ss << std::left << std::setw(40) << missNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Hit rate:";
  ss << std::left << std::setw(40) << hitRate;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Shard Hit num:";
  ss << std::left << std::setw(40) << shardHitNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Shard Miss num:";
  ss << std::left << std::setw(40) << shardMissNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Shard Hit rate:";
  ss << std::left << std::setw(40) << shardHitRate;
  ss << std::endl;

  ss << std::endl;

  return ss.str();
}

const std::shared_ptr<caf::actor_system> &OperatorManager::getActorSystem() const {
  return actorSystem;
}

const caf::actor &OperatorManager::getSegmentCacheActor() const {
  return segmentCacheActor_;
}

const caf::actor &OperatorManager::getDeltaCacheActor() const {
    return deltaCacheActor_;
}

long OperatorManager::nextQueryId() {
  return queryCounter_.fetch_add(1);
}

OperatorManager::~OperatorManager() {
	if(running_)
	  stop();
}

void OperatorManager::clearCacheMetrics() {
  // NOTE: Creating a new scoped_actor will work, but can use rootActor_ as well
  scoped_actor self{*actorSystem};
  // NOTE: anon_send a bit lighter than send
  self->anon_send(segmentCacheActor_, ClearMetricsAtom::value);
}

void OperatorManager::clearCrtQueryMetrics() {
  // NOTE: Creating a new scoped_actor will work, but can use rootActor_ as well
  scoped_actor self{*actorSystem};
  // NOTE: anon_send a bit lighter than send
  self->anon_send(segmentCacheActor_, ClearCrtQueryMetricsAtom::value);
}

void OperatorManager::clearCrtQueryShardMetrics() {
  // NOTE: Creating a new scoped_actor will work, but can use rootActor_ as well
  scoped_actor self{*actorSystem};
// NOTE: anon_send a bit lighter than send
  self->anon_send(segmentCacheActor_, ClearCrtQueryShardMetricsAtom::value);
}

double OperatorManager::getCrtQueryHitRatio() {
  int crtQueryHitNum;
  int crtQueryMissNum;

  auto errorHandler = [&](const caf::error& error){
      throw std::runtime_error(to_string(error));
  };

  // NOTE: Creating a new scoped_actor will work, but can use rootActor_ as well
  scoped_actor self{*actorSystem};
  self->request(segmentCacheActor_, infinite, GetCrtQueryNumHitsAtom::value).receive(
          [&](int numHits) {
              crtQueryHitNum = numHits;
          },
          errorHandler);

  self->request(segmentCacheActor_, infinite, GetCrtQueryNumMissesAtom::value).receive(
          [&](int numMisses) {
              crtQueryMissNum = numMisses;
          },
          errorHandler);

  // NOTE: anon_send a bit lighter than send
  self->anon_send(segmentCacheActor_, ClearCrtQueryMetricsAtom::value);

  return (crtQueryHitNum + crtQueryMissNum == 0) ? 0.0 : (double) crtQueryHitNum / (double) (crtQueryHitNum + crtQueryMissNum);
}

double OperatorManager::getCrtQueryShardHitRatio() {
  int crtQueryShardHitNum;
  int crtQueryShardMissNum;

  auto errorHandler = [&](const caf::error& error){
      throw std::runtime_error(to_string(error));
  };

  // NOTE: Creating a new scoped_actor will work, but can use rootActor_ as well
  scoped_actor self{*actorSystem};
  self->request(segmentCacheActor_, infinite, GetCrtQueryNumShardHitsAtom::value).receive(
          [&](int numShardHits) {
              crtQueryShardHitNum = numShardHits;
          },
          errorHandler);

  self->request(segmentCacheActor_, infinite, GetCrtQueryNumShardMissesAtom::value).receive(
          [&](int numShardMisses) {
              crtQueryShardMissNum = numShardMisses;
          },
          errorHandler);

  // NOTE: anon_send a bit lighter than send
  self->anon_send(segmentCacheActor_, ClearCrtQueryShardMetricsAtom::value);

  return (crtQueryShardHitNum + crtQueryShardMissNum == 0) ? 0.0 : (double) crtQueryShardHitNum / (double) (crtQueryShardHitNum + crtQueryShardMissNum);
}

}