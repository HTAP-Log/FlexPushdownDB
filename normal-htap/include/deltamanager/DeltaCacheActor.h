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
        static std::shared_ptr <LoadDeltasResponseMessage> loadMemoryDeltas(const LoadDeltasRequestMessage &msg,
                                                                      stateful_actor <DeltaCacheActorState> *self);
        static void storeTail(const StoreTailRequestMessage &msg, stateful_actor <DeltaCacheActorState> *self);
    };
}

#endif //NORMAL_DELTACACHEACTOR_H