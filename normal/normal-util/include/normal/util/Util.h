//
// Created by Yifei Yang on 2/9/21.
//

#ifndef NORMAL_UTIL_UTIL_H
#define NORMAL_UTIL_UTIL_H

#include <normal/util/Globals.h>
#include <vector>

namespace normal::util {
  std::string readFile(const std::string& filePath);
  std::vector<std::string> readFileByLine(const std::string& filePath);
  bool isInteger(const std::string& str);
  std::string getLocalIp();
}


#endif //NORMAL_UTIL_UTIL_H
