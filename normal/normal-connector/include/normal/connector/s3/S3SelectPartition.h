//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H
#define NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H

#include <string>

#include <normal/connector/partition/Partition.h>
#include <memory>

class S3SelectPartition: public Partition {
public:
  explicit S3SelectPartition(std::string bucket, std::string object);
  explicit S3SelectPartition(std::string bucket, std::string object, long numBytes);
  S3SelectPartition() = default;
  S3SelectPartition(const S3SelectPartition&) = default;
  S3SelectPartition& operator=(const S3SelectPartition&) = default;

  [[nodiscard]] const std::string &getBucket() const;
  [[nodiscard]] const std::string &getObject() const;

  std::string toString() override;
  std::string type() override;

  size_t hash() override;

  bool equalTo(std::shared_ptr<Partition> other) override;

  bool operator==(const S3SelectPartition& other);

private:
  std::string bucket_;
  std::string object_;

// caf inspect
public:
template <class Inspector>
friend bool inspect(Inspector& f, S3SelectPartition& partition) {
  return f.object(partition).fields(f.field("bucket", partition.bucket_),
                                    f.field("object", partition.object_));
}
};

#endif //NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITION_H
