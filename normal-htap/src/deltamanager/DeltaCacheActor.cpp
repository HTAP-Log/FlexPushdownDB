//
// Created by Elena Milkai on 10/14/21.
//

#include <deltamanager/DeltaCacheActor.h>
#include <deltamanager/GetTailDeltas.h>
#include <normal/connector/MiniCatalogue.h>

using namespace normal::htap::deltamanager;

[[maybe_unused]] behavior DeltaCacheActor::makeBehaviour(stateful_actor<DeltaCacheActorState> *self) {

    self->state.deltasCache = DeltaCache::make();
    self->attach_functor([=](const error &reason) {
        SPDLOG_DEBUG("[Actor {} ('<name unavailable>')]  DeltaActor Cache exit  |  reason: {}",
                     self->id(),
                     to_string(reason));
        self->state.deltasCache.reset();
    });
    return {
            [=](StoreDeltaAtom, const std::shared_ptr<StoreTailRequestMessage> &m) {
                storeTail(*m, self);
            },
            [=](LoadDeltaAtom , const std::shared_ptr<LoadDeltasRequestMessage> &m) {
                return loadMemoryDeltas(*m, self);
            },
    };
}

std::shared_ptr<LoadDeltasResponseMessage> DeltaCacheActor::loadMemoryDeltas(
                              const LoadDeltasRequestMessage &msg,
                              stateful_actor <DeltaCacheActorState> *self){

    SPDLOG_CRITICAL("[4]. DeltaCacheActor: Message of type {} was received from {}.", msg.type(), msg.sender());
    const std::shared_ptr<DeltaCacheKey>& deltaCacheKey = msg.getDeltaKey();
    const std::string tableName = deltaCacheKey->getTableName();
    // TODO: once DeltaPump is called it pumps all the partitions from a table
    auto outputSchema = normal::connector::defaultMiniCatalogue->getDeltaSchema(tableName);
    // allNewDeltas --> tailDeltas
    std::shared_ptr<std::map<int, std::shared_ptr<normal::tuple::TupleSet2>>>
            allNewDeltas = deltamanager::callDeltaPump(outputSchema);
    // Action: we need to merge all partitions, not just the partition for this single query
    for (const auto& delta: *allNewDeltas) {
        int partition = delta.first;
        std::shared_ptr<normal::tuple::TupleSet2> deltaTable = delta.second;
        std::shared_ptr<DeltaCacheKey> key = DeltaCacheKey::make(tableName, partition);
        // TODO: change the timestamp from 0 to a real value
        std::shared_ptr<DeltaCacheData> data = DeltaCacheData::make(deltaTable, 0);
        self->state.deltasCache->store(key, data);
        SPDLOG_CRITICAL("Current size of cache: {}, partition added: {} for {}.",
                        self->state.deltasCache->getSize(), key->getPartition(), key->getTableName());
    }
    // now we load the delta from memory
    auto freshDelta = self->state.deltasCache->load(deltaCacheKey);
    std::vector<std::shared_ptr<TupleSet2>> deltas;
    std::vector<int> timestamps;

    for(const auto& timestampedDelta: freshDelta) {
        deltas.push_back(timestampedDelta->getDelta());
        timestamps.push_back(timestampedDelta->getTimestamp());
        SPDLOG_CRITICAL("Delta:{}, Timestamp:{}", timestampedDelta->getDelta()->toString(), timestampedDelta->getTimestamp());
    }
    std::shared_ptr<LoadDeltasResponseMessage>
            response = std::make_shared<LoadDeltasResponseMessage>(deltas, timestamps, msg.sender());

    SPDLOG_CRITICAL("[5]. DeltaCacheActor: Message of type {} was send to CacheHandler-lineorder-0.", response->type());
    return response;
}

void DeltaCacheActor::storeTail(const StoreTailRequestMessage &msg, stateful_actor <DeltaCacheActorState> *self){
    // periodical update
    // TODO: implement this
}
