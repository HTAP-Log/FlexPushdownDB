//
// Created by matt on 23/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_ACTORS_H
#define NORMAL_NORMAL_CORE_SRC_ACTORS_H

#include <caf/all.hpp>

namespace normal::core {

/**
 * Utility functions for working with actors. A work in progress.
 */
class Actors {

public:
  static caf::actor toActorHandle(const std::shared_ptr<caf::scoped_actor> &a);
  static std::shared_ptr<caf::actor> toActorHandleShared(const std::shared_ptr<caf::scoped_actor> &a);

};

void setDefaultHandlers(::caf::scheduled_actor &self);

}

#endif //NORMAL_NORMAL_CORE_SRC_ACTORS_H
