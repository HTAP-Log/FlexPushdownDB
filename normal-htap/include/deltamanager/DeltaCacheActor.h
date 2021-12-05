//
// Created by Elena Milkai on 10/14/21.
//


#ifndef NORMAL_DELTACACHEACTOR_H
#define NORMAL_DELTACACHEACTOR_H

#include <caf/all.hpp>
#include <deltamanager/DeltaCache.h>
#include <deltamanager/LoadDeltasRequestMessage.h>
#include <deltamanager/LoadDeltasResponseMessage.h>
#include <deltamanager/StoreTailRequestMessage.h>
#include <normal/core/message/TupleMessage.h>
#include <string>

using namespace caf;

namespace normal::htap::deltamanager {

    struct DeltaCacheActorState {
        std::string name = "deltas-cache";
        std::shared_ptr<DeltaCache> deltasCache;
    };

    using StoreDeltaAtom = atom_constant<atom("StoreDelta")>;
    using LoadDeltaAtom = atom_constant<atom("LoadDeltas")>;

    class DeltaCacheActor {
    public:
        /**
         * The function decides the behavior of the actor based on the message that is receiving.
         * @param self
         * @return behavior fo the actor.
         */
        [[maybe_unused]] static behavior makeBehaviour(caf::stateful_actor<DeltaCacheActorState> *self);

        /**
         * Request for deltas coming from the DeltaMerge operators. This will also trigger a call from DeltaPump
         * as an instant tail update.
         * @param msg
         * @param self
         * @return the response with the requested deltas (memory and tail).
         */
        static std::shared_ptr<LoadDeltasResponseMessage> loadMemoryDeltas(const LoadDeltasRequestMessage &msg,
                                                                      stateful_actor <DeltaCacheActorState> *self);
        /**
         * Request for getting the tail, this is periodically triggered from another actor.
         * @param msg
         * @param self
         */
        static void storeTail(const StoreTailRequestMessage &msg, stateful_actor <DeltaCacheActorState> *self);
    };
}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::htap::deltamanager::LoadDeltasRequestMessage>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::htap::deltamanager::LoadDeltasResponseMessage>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::htap::deltamanager::StoreTailRequestMessage>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::core::message::TupleMessage>)

#endif //NORMAL_DELTACACHEACTOR_H