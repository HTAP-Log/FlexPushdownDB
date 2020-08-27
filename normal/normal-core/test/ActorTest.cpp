//
// Created by matt on 21/8/20.
//

#include <doctest/doctest.h>
#include <caf/all.hpp>

#include <normal/core/SystemActor.h>
#include <normal/core/cache/SegmentCacheActor2.h>
#include <normal/core/QueryExecutorActor.h>
#include <normal/core/Actors.h>
#include <normal/core/Normal.h>
#include "SimpleOperator.h"
#include "ErrorOperator.h"

using namespace caf;
using namespace normal::core;
using namespace normal::core::cache;

TEST_SUITE ("normal") {

TEST_CASE ("normal-lifecycle" * doctest::skip(false)) {
  auto n = Normal::start();

  auto q = n->createQuery();
  q->put(std::make_shared<SimpleOperator>("simple-1"));
  q->put(std::make_shared<SimpleOperator>("simple-2"));
  q->put(std::make_shared<SimpleOperator>("simple-3"));

  auto r = n->execute(q);

  SPDLOG_DEBUG("Result:\n{}", r.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  n->stop();
}

TEST_CASE ("normal-error" * doctest::skip(false)) {
  auto n = Normal::start();

  auto q = n->createQuery();
  q->put(std::make_shared<SimpleOperator>("simple-1"));
  q->put(std::make_shared<SimpleOperator>("simple-2"));
  q->put(std::make_shared<ErrorOperator>("error-3"));

  auto r = n->execute(q);

  if(r.has_value())
  	SPDLOG_DEBUG("Result:\n{}", r.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  else
	SPDLOG_DEBUG("Error: {}", r.error());

  n->stop();
}

TEST_CASE ("normal-lifecycle-load" * doctest::skip(false)) {
  auto n = Normal::start();

  for(int i=0;i<1000;++i) {
	auto q = n->createQuery();
	q->put(std::make_shared<SimpleOperator>("simple-1"));
	q->put(std::make_shared<SimpleOperator>("simple-2"));
	q->put(std::make_shared<SimpleOperator>("simple-3"));

	auto r = n->execute(q);

	if (r.has_value())
	  SPDLOG_DEBUG("Result:\n{}", r.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
	else
	  SPDLOG_DEBUG("Error: {}", r.error());
  }

  n->stop();
}

TEST_CASE ("normal-error-load" * doctest::skip(false)) {
  auto n = Normal::start();

  for(int i=0;i<1000;++i) {
	auto q = n->createQuery();
	q->put(std::make_shared<SimpleOperator>("simple-1"));
	q->put(std::make_shared<SimpleOperator>("simple-2"));
	q->put(std::make_shared<ErrorOperator>("error-3"));

	auto r = n->execute(q);

	if (r.has_value())
	  SPDLOG_DEBUG("Result:\n{}", r.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
	else
	  SPDLOG_DEBUG("Error: {}", r.error());
  }

  n->stop();
}

}
