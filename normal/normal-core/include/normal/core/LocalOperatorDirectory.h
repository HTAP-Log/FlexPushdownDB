//
// Created by matt on 25/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORY_H
#define NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORY_H

#include <unordered_map>
#include <string>
#include <tl/expected.hpp>

#include "LocalOperatorDirectoryEntry.h"

namespace normal::core {

/**
 * A directory that operators use to store information about other operators
 */
class LocalOperatorDirectory {

private:
  std::unordered_map <std::string, LocalOperatorDirectoryEntry> entries_;

  int numProducers = 0;
  int numConsumers = 0;
  int numProducersComplete = 0;
  int numConsumersComplete = 0;


public:
  LocalOperatorDirectory() = default;
  LocalOperatorDirectory(const LocalOperatorDirectory&) = default;
  LocalOperatorDirectory& operator=(const LocalOperatorDirectory&) = default;

  void insert(const LocalOperatorDirectoryEntry &entry);
  void setComplete(const std::string &name);
//  bool allComplete();
  bool allComplete(const OperatorRelationshipType &operatorRelationshipType);
  [[nodiscard]] std::string showString() const;
  void setIncomplete();
  void clearForSegmentCache();
  void clearUsingEmpty();

  tl::expected<LocalOperatorDirectoryEntry, std::string>
  get(const std::string& operatorId);
  std::vector<LocalOperatorDirectoryEntry>
  get(const OperatorRelationshipType &operatorRelationshipType);
  void destroyActorHandles();

// caf inspect
public:
template <class Inspector>
friend bool inspect(Inspector& f, LocalOperatorDirectory& directory) {
  return f.object(directory).fields(f.field("entries", directory.entries_),
                                    f.field("numProducers", directory.numProducers),
                                    f.field("numConsumers", directory.numConsumers),
                                    f.field("numProducersComplete", directory.numProducersComplete),
                                    f.field("numConsumersComplete", directory.numConsumersComplete));
}
};

}

#endif //NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORY_H
