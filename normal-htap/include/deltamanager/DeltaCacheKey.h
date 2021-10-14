//
// Created by Elena Milkai on 10/14/21.
//

#ifndef NORMAL_DELTACACHEKEY_H
#define NORMAL_DELTACACHEKEY_H

namespace normal::htap::deltamanager{

    class DeltaCacheKey{
    public:
        explicit DeltaCacheKey(const std::string& tableName, const int& partition, const int& timestamp);
        static std::shared_ptr<DeltaCacheKey> make(const std::string& tableName, const int& partition, const int& timestamp);
        size_t hash();
        bool operator==(const DeltaKey& other) const;
        bool operator!=(const DeltaKey& other) const;
        [[nodiscard]] const std::string &getTableName() const;
        [[nodiscard]] const int &getPartition() const;
        [[nodiscard]] const int &getTimestamp() const;
    private:
        std::string tableName_;
        int timestamp_;
        int partition_;
    };

    struct DeltaKeyPointerHash {
        inline size_t operator()(const std::shared_ptr<DeltaKey> &key) const {
            return key->hash();
        }
    };

    struct DeltaKeyPointerPredicate {
        inline bool operator()(const std::shared_ptr<DeltaKey>& lhs, const std::shared_ptr<DeltaKey>& rhs) const {
            return *lhs == *rhs;
        }
    };
}

#endif //NORMAL_DELTACACHEKEY_H