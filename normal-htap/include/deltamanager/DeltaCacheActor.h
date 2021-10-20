//
// Created by Elena Milkai on 10/14/21.
//


#ifndef NORMAL_DELTASCACHEACTOR_H
#define NORMAL_DELTASCACHEACTOR_H

#include <normal/core/Operator.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/core/message/TupleMessage.h>
#include <normal/tuple/TupleSet2.h>
#include <string>

using namespace normal::avro_tuple::make;
using namespace normal::htap::deltamanager;

namespace normal::htap::deltamanager {

    struct DeltasCacheActorState {
        std::string name = "deltas-cache";
        std::shared_ptr<DeltasCache> deltasCache;
    };

    class DeltasCacheActor {
    public:
        [[maybe_unused]] static behavior makeBehaviour(stateful_actor <DeltasCacheActorState> *self,
                                                       const std::optional <std::shared_ptr<CachingPolicy>> &cachingPolicy);

        /**
         * This function returns all the memory deltas based on the specified table schema and partition.
         * @param msg
         * @param self
         * @return
         */
        static std::shared_ptr <LoadResponseMessage> loadMemoryDeltas(const LoadRequestMessage &msg,
                                                                      stateful_actor <DeltasCacheActorState> *self);

        static void storeTail(const StoreRequestMessage &msg, stateful_actor <DeltasCacheActorState> *self);




    };
}

#endif //NORMAL_DELTASCACHEACTOR_H