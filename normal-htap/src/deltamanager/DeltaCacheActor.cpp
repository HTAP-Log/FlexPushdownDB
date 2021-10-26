//
// Created by Elena Milkai on 10/14/21.
//

#include <deltamanager/DeltaCacheActor.h>
#include <deltamanager/StoreDeltaRequestMessage.h>
#include <deltamanager/LoadDeltaResponseMessage.h>
#include <deltamanager/LoadDeltaRequestMessage.h>

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
            [=](StoreDeltaAtom, const std::shared_ptr<StoreDeltaRequestMessage> &m) {
                storeTail(*m, self);
            }
    };
}


/*std::shared_ptr <LoadDeltaResponseMessage> DeltaCacheActor::loadMemoryDeltas(const LoadDeltaRequestMessage &msg,
                                                                   stateful_actor <DeltaCacheActorState> *self){

}*/

void DeltaCacheActor::storeTail(const StoreDeltaRequestMessage &msg, stateful_actor <DeltaCacheActorState> *self){
    SPDLOG_DEBUG("Store  |  storeMessage: {}", msg.toString());
    auto key = msg.getTailDelta().begin()->first;
    auto data = msg.getTailDelta().begin()->second;
    self->state.deltasCache->store(key, data);

}
