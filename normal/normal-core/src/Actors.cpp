//
// Created by matt on 23/3/20.
//

#include "normal/core/Actors.h"

#include <caf/all.hpp>

#include <normal/core/Globals.h>

namespace normal::core {

caf::actor Actors::toActorHandle(const std::shared_ptr<caf::scoped_actor> &a) {
  return caf::actor_cast<caf::actor>(*a);
}

std::shared_ptr<caf::actor> Actors::toActorHandleShared(const std::shared_ptr<caf::scoped_actor> &a) {
  return reinterpret_cast<const std::shared_ptr<caf::actor> &>(*a);
}

void setDefaultHandlers(::caf::scheduled_actor &self) {

  SPDLOG_DEBUG("Actor Spawn  |  actor: {} ('{}')", self.id(), self.name());

  self.set_error_handler([&](const ::caf::error &e) {
	SPDLOG_ERROR("Actor Error  |  actor: {} ('{}'), error: {}", self.id(),
				 self.name(), to_string(e));
  });

  self.set_down_handler([&](const ::caf::down_msg &m) {
	SPDLOG_WARN("Actor Down  |  receiver: {} ('{}'), down actor: {}, down reason: {}", self.id(),
				 self.name(), to_string(m.source), to_string(m.reason));
  });

  self.set_exit_handler([&](const ::caf::exit_msg &m) {
	SPDLOG_DEBUG("Actor Exit  |  receiver: {} ('{}'), exit sender: {}, exit reason: {}", self.id(),
				 self.name(), to_string(m.source), to_string(m.reason));
	self.quit(m.reason);
  });

  self.set_default_handler([&](::caf::scheduled_actor* a, ::caf::message_view& m) {
	SPDLOG_WARN("Actor Unexpected Message  |  actor: {} ('{}'), message: {}", self.id(),
				 self.name(), to_string(m.copy_content_to_message()));
	return m.move_content_to_message();
  });

  self.set_exception_handler([&](std::exception_ptr &e) -> ::caf::error {
	try {
	  if (e) {
		std::rethrow_exception(e);
	  }
	} catch (const std::exception &e) {
	  SPDLOG_ERROR("Actor Unhandled Exception  |  actor: {} ('{}'), cause: '{}'",
				   self.id(),
				   self.name(),
				   e.what());
	}

	return make_error(::caf::exit_reason::unhandled_exception);
  });
}

}
