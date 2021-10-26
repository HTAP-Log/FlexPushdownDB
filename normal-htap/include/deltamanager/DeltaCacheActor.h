//
// Created by Elena Milkai on 10/14/21.
//


#ifndef NORMAL_DELTACACHEACTOR_H
#define NORMAL_DELTACACHEACTOR_H

#include <caf/all.hpp>
#include <deltamanager/DeltaCache.h>
#include <deltamanager/StoreDeltaRequestMessage.h>
#include <deltamanager/LoadDeltaRequestMessage.h>
#include <deltamanager/LoadDeltaResponseMessage.h>
#include <string>

using namespace caf;

namespace normal::htap::deltamanager {

    struct DeltaCacheActorState {
        std::string name = "deltas-cache";
        std::shared_ptr<DeltaCache> deltasCache;
    };

    using StoreDeltaAtom = atom_constant<atom("StoreDelta")>;
    using LoadDeltaAtom = atom_constant<atom("LoadDelta")>;

    class DeltaCacheActor {
    public:
        [[maybe_unused]] behavior makeBehaviour(caf::stateful_actor<DeltaCacheActorState> *self);
        static std::shared_ptr <LoadDeltaResponseMessage> loadMemoryDeltas(const LoadDeltaRequestMessage &msg,
                                                                      stateful_actor <DeltaCacheActorState> *self);
        static void storeTail(const StoreDeltaRequestMessage &msg, stateful_actor <DeltaCacheActorState> *self);
    };
}

#endif //NORMAL_DELTACACHEACTOR_H