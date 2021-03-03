//
// Created by matt on 25/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORYENTRY_H
#define NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORYENTRY_H

#include <string>
#include <caf/all.hpp>
#include "OperatorRelationshipType.h"

namespace normal::core {

/**
 * An entry in the local operator directory
 */
class LocalOperatorDirectoryEntry {

private:
  std::string name_;
  caf::actor actor_;
  OperatorRelationshipType relationshipType_;
  bool complete_;

public:
  LocalOperatorDirectoryEntry(std::string name,
                              caf::actor actor,
                              OperatorRelationshipType relationshipType,
                              bool complete);
  LocalOperatorDirectoryEntry() = default;
  LocalOperatorDirectoryEntry(const LocalOperatorDirectoryEntry&) = default;
  LocalOperatorDirectoryEntry& operator=(const LocalOperatorDirectoryEntry&) = default;

  [[nodiscard]] bool complete() const;
  void complete(bool complete);

  [[nodiscard]] const std::string &name() const;
  void name(const std::string &name);
  [[nodiscard]] const caf::actor &getActor() const;
  void destroyActor();
  [[nodiscard]] OperatorRelationshipType relationshipType() const;
  void relationshipType(OperatorRelationshipType relationshipType);

// caf inspect
public:
template <class Inspector>
friend bool inspect(Inspector& f, LocalOperatorDirectoryEntry& entry) {
  return f.object(entry).fields(f.field("name", entry.name_),
                                f.field("actor", entry.actor_),
                                f.field("relationshipType", entry.relationshipType_),
                                f.field("complete", entry.complete_));
}
};

}

#endif //NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORYENTRY_H
