//
// Created by matt on 24/3/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORDIRECTORY_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORDIRECTORY_H

#include <string>
#include <unordered_map>
#include "OperatorDirectoryEntry.h"

namespace normal::core {

/**
 * Class for tracking operators from outside the actor system
 */
class OperatorDirectoryEntry;

class OperatorDirectory {
private:
  std::unordered_map<std::string, OperatorDirectoryEntry> entries_;

public:
  void insert(const OperatorDirectoryEntry& entry);
  void setComplete(const std::string& name);
  bool allComplete();
  std::string showString() const;
  void setIncomplete();

};

}
#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORDIRECTORY_H
