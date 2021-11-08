//
// Created by Elena Milkai on 10/14/21.
//

#ifndef NORMAL_DELTACACHEKEY_H
#define NORMAL_DELTACACHEKEY_H

#include <string>
#include <memory>
#include <fmt/format.h>

namespace normal::htap::deltamanager{

    class DeltaCacheKey{
    public:
        explicit DeltaCacheKey(const std::string& tableName, const int& partition);
        static std::shared_ptr<DeltaCacheKey> make(const std::string& tableName, const int& partition);
        size_t hash();
        int tableToVector();
        bool operator==(const DeltaCacheKey& other) const;
        bool operator!=(const DeltaCacheKey& other) const;
        [[nodiscard]] const std::string &getTableName() const;
        [[nodiscard]] const int &getPartition() const;

        std::string toString();
    private:
        std::string tableName_;
        int partition_;
    };

    struct DeltaKeyPointerHash {
        inline size_t operator()(const std::shared_ptr<DeltaCacheKey> &key) const {
            return key->hash();
        }
    };

    struct DeltaKeyPointerPredicate {
        inline bool operator()(const std::shared_ptr<DeltaCacheKey>& lhs, const std::shared_ptr<DeltaCacheKey>& rhs) const {
            return *lhs == *rhs;
        }
    };
}

#endif //NORMAL_DELTACACHEKEY_H