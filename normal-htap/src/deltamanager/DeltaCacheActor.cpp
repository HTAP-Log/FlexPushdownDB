//
// Created by Elena Milkai on 10/14/21.
//

#include "normal/htap/deltamanager/DeltaCacheActor.h"

namespace normal::htap::deltamanager{

    [[maybe_unused]] behavior DeltaCacheActor::makeBehaviour(stateful_actor<DeltaCacheActorState> *self) {

        self->state.cache = DeltaCache::make();

        /**
         * Handler for actor exit event
         */
        self->attach_functor([=](const caf::error &reason) {

            // FIXME: Actor name appears to have been destroyed by this stage, it
            //  often comes out as garbage anyway, so we avoid using it. Something
            //  to raise with developers.
            SPDLOG_DEBUG("[Actor {} ('<name unavailable>')]  Segment Cache exit  |  reason: {}",
                         self->id(),
                         to_string(reason));

            self->state.cache.reset();
        });

        return {
                [=](StoreAtom, const std::shared_ptr<StoreRequestMessage> &m) {
                    store(*m, self);
                }
        };
    }

    void DeltaCacheActor::storeTail(const StoreTailRequestMessage &msg, stateful_actor <DeltasCacheActorState> *self){
        SPDLOG_DEBUG("Store  |  storeMessage: {}", msg.toString());
        auto key = msg.getTailDelta().first;
        auto data = msg.getTailDelta().second;
        self->state.cache->store(key, data);

    }

}