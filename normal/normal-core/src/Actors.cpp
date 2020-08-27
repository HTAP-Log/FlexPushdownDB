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

  self.set_error_handler([&](const ::caf::error &e) {
	SPDLOG_DEBUG("[Actor {} ('{}')]  Actor Error  |  error: {}", self.id(),
				 self.name(), to_string(e));
  });

  self.set_down_handler([&](const ::caf::down_msg &m) {
	SPDLOG_DEBUG("[Actor {} ('{}')]  Actor Down  |  source: {}, reason: {}", self.id(),
				 self.name(), to_string(m.source), to_string(m.reason));
  });

  self.set_exit_handler([&](const ::caf::exit_msg &m) {
	if(m.reason != ::caf::exit_reason::normal) {
	  SPDLOG_WARN("[Actor {} ('{}')]  Actor Exit  |  source: {}, reason: {}", self.id(),
				   self.name(), to_string(m.source), to_string(m.reason));
	  self.quit(m.reason);
	}
	else{
	  SPDLOG_DEBUG("[Actor {} ('{}')]  Actor Exit  |  source: {}, reason: {}", self.id(),
				   self.name(), to_string(m.source), to_string(m.reason));
	}
  });

  self.set_default_handler([&](::caf::scheduled_actor* a, ::caf::message_view& m) {
	SPDLOG_WARN("[Actor {} ('{}')]  Unexpected Message  |  message: {}", self.id(),
				 self.name(), to_string(m.copy_content_to_message()));
	return m.move_content_to_message();
  });

  self.set_exception_handler([&](std::exception_ptr &e) -> ::caf::error {

    std::string msg;

	try {
	  if (e) {
		std::rethrow_exception(e);
	  }
	} catch (const std::exception &e) {
	  msg = fmt::format("[Actor {} ('{}')]  Unhandled Exception  |  cause: '{}'",
				   self.id(),
				   self.name(),
				   e.what());
	}

	return make_error(::caf::sec::runtime_error, msg);
  });
}

}
