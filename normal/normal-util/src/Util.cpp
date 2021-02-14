//
// Created by Yifei Yang on 2/9/21.
//

#include <normal/util/Util.h>
#include <fstream>
#include <sstream>

using namespace normal::util;

std::string normal::util::readFile(std::string filePath) {
  std::ifstream ifile(filePath);
  std::ostringstream buf;
  char ch;
  while (buf && ifile.get(ch)) {
    buf.put(ch);
  }
  return buf.str();
}
