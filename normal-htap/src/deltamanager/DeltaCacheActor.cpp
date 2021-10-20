//
// Created by Elena Milkai on 10/14/21.
//

#include <deltamanager/DeltaCacheActor.h>
#include <deltamanager/LoadDeltasResponseMessage.h>
#include <deltamanager/LoadDeltasRequestMessage.h>
#include <deltamanager/StoreTailRequestMessage.h>

using namespace normal::htap::deltamanager;

[[maybe_unused]] behavior DeltaCacheActor::makeBehaviour(stateful_actor<DeltaCacheActorState> *self) {

    self->state.deltasCache = DeltaCache::make();

    /**
     * Handler for actor exit event
     */
    self->attach_functor([=](const error &reason) {

        // FIXME: Actor name appears to have been destroyed by this stage, it
        //  often comes out as garbage anyway, so we avoid using it. Something
        //  to raise with developers.
        SPDLOG_DEBUG("[Actor {} ('<name unavailable>')]  Segment Cache exit  |  reason: {}",
                     self->id(),
                     to_string(reason));

        self->state.deltasCache.reset();
    });

    return {
            [=](StoreDeltaAtom, const std::shared_ptr<StoreTailRequestMessage> &m) {
                storeTail(*m, self);
            },
            [=](LoadDeltaAtom , const std::shared_ptr<LoadDeltasRequestMessage> &m) {
                loadMemoryDeltas(*m, self);
            },
    };
}


std::shared_ptr <LoadDeltasResponseMessage> DeltaCacheActor::loadMemoryDeltas(
        const LoadDeltasRequestMessage &msg,
        stateful_actor <DeltaCacheActorState> *self){
}

void DeltaCacheActor::storeTail(const StoreTailRequestMessage &msg, stateful_actor <DeltaCacheActorState> *self){
    //SPDLOG_DEBUG("Store  |  storeMessage: {}", msg.toString());
    //auto key = msg.getTailKey().begin()->first;
    //self->state.deltasCache->store(key, data);

}
