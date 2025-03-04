//
// Created by matt on 23/9/20.
//

#include "normal/pushdown/file/FileScan2.h"

#include <utility>

namespace normal::pushdown::file {

FileScanActor::behavior_type FileScanFunctor(FileScanStatefulActor self,
											 std::string name,
											 const std::string &filePath,
											 FileType fileType,
											 const std::vector<std::string> &columnNames,
											 unsigned long startOffset,
											 unsigned long finishOffset,
											 long queryId,
											 const caf::actor& rootActorHandle,
											 const caf::actor& segmentCacheActorHandle,
                                             const caf::actor& deltaCacheActorHandle,
											 bool scanOnStart) {

  self->state.setState(self,
					   std::move(name),
					   filePath,
					   fileType,
					   columnNames,
					   startOffset,
					   finishOffset,
					   queryId,
					   rootActorHandle,
					   segmentCacheActorHandle,
                       deltaCacheActorHandle,
					   scanOnStart);

  return self->state.makeBehavior(self);
}

}
