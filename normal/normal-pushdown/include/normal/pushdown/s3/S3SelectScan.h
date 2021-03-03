//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
#define NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H

#include <memory>
#include <string>

#include <aws/s3/S3Client.h>

#include <normal/pushdown/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/pushdown/scan/ScanMessage.h>
#include <normal/core/cache/WeightRequestMessage.h>

#include "normal/core/Operator.h"
#include "normal/tuple/TupleSet.h"
#include "normal/pushdown/s3/S3SelectCSVParseOptions.h"
#include "normal/pushdown/s3/S3CSVParser.h"

using namespace normal::core;
using namespace normal::core::message;
using namespace normal::core::cache;

namespace normal::pushdown {

class S3SelectScan : public normal::core::Operator {
public:
  S3SelectScan() = default;
  S3SelectScan(const S3SelectScan&) = default;
  S3SelectScan& operator=(const S3SelectScan&) = default;
  S3SelectScan(std::string name,
			   std::string type,
			   std::string s3Bucket,
			   std::string s3Object,
			   std::vector<std::string> returnedS3ColumnNames,
			   std::vector<std::string> neededColumnNames,
			   int64_t startOffset,
			   int64_t finishOffset,
         std::string tableName,
			   bool scanOnStart,
			   bool toCache,
			   long queryId,
         std::vector<std::shared_ptr<normal::cache::SegmentKey>> weightedSegmentKeys);

  // Something has to be done after Operator created to avoid serialization
  void makeSchema();
  void reserveColumnsReadFromS3();

  [[nodiscard]] size_t getProcessedBytes() const;
  [[nodiscard]] size_t getReturnedBytes() const;
  [[nodiscard]] size_t getNumRequests() const;

  [[nodiscard]] size_t getGetTransferTimeNS() const;
  [[nodiscard]] size_t getGetConvertTimeNS() const;
  [[nodiscard]] size_t getSelectTransferAndConvertTimeNS() const;
  [[nodiscard]] size_t getSelectConvertTimeNS() const;

  // A series of get functions
  const std::string &getS3Bucket() const;
  const std::string &getS3Object() const;
  const std::vector<std::string> &getReturnedS3ColumnNames() const;
  const std::vector<std::string> &getNeededColumnNames() const;
  const int64_t& getStartOffset() const;
  const int64_t& getFinishOffset() const;
  const std::shared_ptr<arrow::Schema> &getSchema() const;
  const bool& isScanOnStart() const;
  const bool& isToCache() const;
  const std::vector<std::shared_ptr<normal::cache::SegmentKey>> &getWeightedSegmentKeys() const;

protected:
  std::string s3Bucket_;
  std::string s3Object_;
  std::vector<std::string> returnedS3ColumnNames_;
	std::vector<std::string> neededColumnNames_;
  int64_t startOffset_;
  int64_t finishOffset_;
  std::string tableName_;
  std::shared_ptr<arrow::Schema> schema_;
  // the columns that are read from s3 (SELECT vs GET will differ depending on the projection for Select)
  std::vector<std::shared_ptr<std::pair<std::string, ::arrow::ArrayVector>>> columnsReadFromS3_;
  size_t processedBytes_ = 0;
  size_t returnedBytes_ = 0;
  size_t numRequests_ = 0;

  size_t getTransferTimeNS_ = 0;
  size_t getConvertTimeNS_ = 0;

  size_t selectTransferAndConvertNS_ = 0;
  size_t selectConvertTimeNS_ = 0;

  /**
   * Flags:
   * 1) scanOnStart_: send request to s3 at the beginning
   * 2) toCache_: whether the segments are to be cached
   */
  bool scanOnStart_;
  bool toCache_;

  void onStart();
  void onReceive(const Envelope &message) override;
  void onCacheLoadResponse(const scan::ScanMessage &message);
  virtual void processScanMessage(const scan::ScanMessage &message) = 0;

  void put(const std::shared_ptr<TupleSet2> &tupleSet);

  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet);
  virtual std::shared_ptr<TupleSet2> readTuples() = 0;
  virtual int getPredicateNum() = 0;
  virtual void readAndSendTuples();
  void sendSegmentWeight();

  /**
   * used to compute filter weight
   */
  std::vector<std::shared_ptr<normal::cache::SegmentKey>> weightedSegmentKeys_;
};

}

#endif //NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
