//
// Created by Yifei Yang on 2/20/21.
//

#include <normal/core/Globals.h>
#include <normal/core/serialization/OperatorSer.h>
#include <normal/core/OperatorContext.h>
#include <normal/core/OperatorActor.h>
#include <normal/core/OperatorActor2.h>
#include <normal/core/cache/SegmentCacheActor.h>
#include <normal/core/message/Envelope.h>
#include <normal/core/serialization/MessageSer.h>
#include <normal/core/serialization/TypeSer.h>

#include <normal/pushdown/serialization/AggregationFunctionSer.h>
#include <normal/pushdown/collate/Collate2.h>
#include <normal/pushdown/s3/S3SelectScan2.h>
#include <normal/pushdown/ScanOperator.h>
#include <normal/pushdown/file/FileScan2.h>
#include <normal/pushdown/group/GroupKernel2.h>

#include <normal/cache/SegmentKey.h>
#include <normal/cache/SegmentMetadata.h>

#include <normal/connector/serialization/PartitionSer.h>

#include <normal/expression/gandiva/serialization/ExpressionSer.h>

#include <normal/tuple/TupleSet.h>

void normal::core::init_caf_global_meta_objects() {
  ::caf::init_global_meta_objects<::caf::id_block::Operator>();
  ::caf::init_global_meta_objects<::caf::id_block::OperatorContext>();
  ::caf::init_global_meta_objects<::caf::id_block::OperatorActor>();
  ::caf::init_global_meta_objects<::caf::id_block::OperatorActor2>();
  ::caf::init_global_meta_objects<::caf::id_block::SegmentCacheActor>();
  ::caf::init_global_meta_objects<::caf::id_block::Envelope>();
  ::caf::init_global_meta_objects<::caf::id_block::Message>();
  ::caf::init_global_meta_objects<::caf::id_block::Type>();

  ::caf::init_global_meta_objects<::caf::id_block::AggregationFunction>();
  ::caf::init_global_meta_objects<::caf::id_block::Collate2>();
  ::caf::init_global_meta_objects<::caf::id_block::S3SelectScan2>();
  ::caf::init_global_meta_objects<::caf::id_block::ScanOperator>();
  ::caf::init_global_meta_objects<::caf::id_block::FileScan2>();
  ::caf::init_global_meta_objects<::caf::id_block::GroupKernel2>();

  ::caf::init_global_meta_objects<::caf::id_block::SegmentKey>();
  ::caf::init_global_meta_objects<::caf::id_block::SegmentMetadata>();

  ::caf::init_global_meta_objects<::caf::id_block::Partition>();

  ::caf::init_global_meta_objects<::caf::id_block::Expression>();

  ::caf::init_global_meta_objects<::caf::id_block::TupleSet>();

  ::caf::core::init_global_meta_objects();
  ::caf::io::middleman::init_global_meta_objects();
}

