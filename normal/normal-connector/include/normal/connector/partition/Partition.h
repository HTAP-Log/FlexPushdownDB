//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_PARTITION_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_PARTITION_H

#include <string>

/**
 * Base class for partition meta data
 *
 * TODO: Just a placeholder at the moment
 */
class Partition {
public:
  virtual ~Partition() = default;

  virtual std::string toString() = 0;

};

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_PARTITION_H
