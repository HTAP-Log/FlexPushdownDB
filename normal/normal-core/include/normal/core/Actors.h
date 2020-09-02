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

/**
 * Makes a request against the given destination actor and waits for the response.
 * Wraps the response in an expected.
 * The destination actor cannot return an expected (otherwise would get expecteds within expecteds).
 *
 * @tparam ReturnType
 * @tparam Dest
 * @tparam Args
 * @param src
 * @param dest
 * @param args
 * @return
 */
template<typename ReturnType, typename Dest, typename... Args>
std::enable_if_t<!tl::detail::is_expected_impl<ReturnType>::value, tl::expected<ReturnType, std::string>>
request(const caf::scoped_actor &src, const Dest &dest, Args... args) {

  tl::expected<ReturnType, std::string> expected;

  src->request(dest, ::caf::infinite, args...).receive(
	  [&](const ReturnType &r) {
		expected = r;
	  },
	  [&](const ::caf::error &err) {
		SPDLOG_ERROR("[Actor {} ('{}')]  Request Error  |  source: {}, reason: {}", src->id(),
					 src->name(), to_string(dest), to_string(err));
		expected = tl::make_unexpected(to_string(err));
	  }
  );

  return expected;
}

/**
 * Makes a request against the given destination actor and waits for the response.
 * The destination actor should return an expected.
 *
 * @tparam ReturnType
 * @tparam Dest
 * @tparam Args
 * @param src
 * @param dest
 * @param args
 * @return
 */
template<typename ReturnType, typename Dest, typename... Args>
std::enable_if_t<tl::detail::is_expected_impl<ReturnType>::value, ReturnType>
request(const caf::scoped_actor &src, const Dest &dest, Args... args) {

  ReturnType expected;

  src->request(dest, ::caf::infinite, args...).receive(
	  [&](const ReturnType &r) {
		expected = r;
	  },
	  [&](const ::caf::error &err) {
		SPDLOG_ERROR("[Actor {} ('{}')]  Request Error  |  source: {}, reason: {}", src->id(),
					 src->name(), to_string(dest), to_string(err));
		expected = tl::make_unexpected(to_string(err));
	  }
  );

  return expected;
}

}

#endif //NORMAL_NORMAL_CORE_SRC_ACTORS_H
