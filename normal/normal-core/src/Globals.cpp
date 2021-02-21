//
// Created by Yifei Yang on 2/20/21.
//

#include <normal/core/Globals.h>
#include <normal/core/OperatorActor.h>
#include <normal/core/OperatorActor2.h>
#include <normal/core/cache/SegmentCacheActor.h>
#include <normal/pushdown/collate/Collate2.h>
#include <normal/pushdown/s3/S3SelectScan2.h>
#include <normal/pushdown/ScanOperator.h>
#include <normal/pushdown/file/FileScan2.h>

void normal::core::init_caf_global_meta_objects() {
  ::caf::init_global_meta_objects<::caf::id_block::OperatorActor>();
  ::caf::init_global_meta_objects<::caf::id_block::OperatorActor2>();
  ::caf::init_global_meta_objects<::caf::id_block::SegmentCacheActor>();
  ::caf::init_global_meta_objects<::caf::id_block::Collate2>();
  ::caf::init_global_meta_objects<::caf::id_block::S3SelectScan2>();
  ::caf::init_global_meta_objects<::caf::id_block::ScanOperator>();
  ::caf::init_global_meta_objects<::caf::id_block::FileScan2>();
  ::caf::core::init_global_meta_objects();
  ::caf::io::middleman::init_global_meta_objects();
}

