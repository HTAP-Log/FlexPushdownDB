//
// Created by matt on 23/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_ACTORS_H
#define NORMAL_NORMAL_CORE_SRC_ACTORS_H

#include <caf/all.hpp>
#include <tl/expected.hpp>

#include <normal/core/Globals.h>

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

template<typename R, typename D, typename... Args>
tl::expected<R, std::string> request(const caf::scoped_actor &src, const D &dest, Args... args) {

  tl::expected<R, std::string> expectedR;

  src->request(dest, ::caf::infinite, args...).receive(
	  [&](const R &r) {
		expectedR = r;
	  },
	  [&](const ::caf::error &err) {
		SPDLOG_ERROR("[Actor {} ('{}')]  Request Error  |  source: {}, reason: {}", src->id(),
					 src->name(), to_string(dest), to_string(err));
		expectedR = tl::make_unexpected(to_string(err));
	  }
  );

  return expectedR;
}

}

#endif //NORMAL_NORMAL_CORE_SRC_ACTORS_H
