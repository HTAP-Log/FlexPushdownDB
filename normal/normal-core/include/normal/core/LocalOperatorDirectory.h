//
// Created by matt on 25/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORY_H
#define NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORY_H

#include <unordered_map>
#include <string>

#include "LocalOperatorDirectoryEntry.h"

namespace normal::core {

/**
 * A directory that operators use to store information about other operators
 */
class LocalOperatorDirectory {

private:
  std::unordered_map <std::string, LocalOperatorDirectoryEntry> entries_;

public:
  void insert(const LocalOperatorDirectoryEntry &entry);
  void setComplete(const std::string &name);
  bool allComplete();
  bool allComplete(const OperatorRelationshipType &operatorRelationshipType);
  std::string showString() const;
  void setIncomplete();

};

}

#endif //NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORY_H
