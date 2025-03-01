//
// Created by Elena Milkai on 10/14/21.
//

#ifndef NORMAL_DELTACACHEDATA_H
#define NORMAL_DELTACACHEDATA_H

#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;

namespace normal::htap::deltamanager {
    class DeltaCacheData {
    public:
        explicit DeltaCacheData(std::shared_ptr<TupleSet2> delta, const int& timestamp);
        static std::shared_ptr<DeltaCacheData> make(const std::shared_ptr<TupleSet2> &delta, const int& timestamp);
        [[nodiscard]] const std::shared_ptr<TupleSet2> &getDelta() const;
        [[nodiscard]] const int &getTimestamp() const;

    private:
        /**
         * Each cache unit includes the delta (coming from the tail) and the version/timestamp of it.
         */
        std::shared_ptr<TupleSet2> delta_;
        int timestamp_;
    };
}

#endif //NORMAL_DELTACACHEDATA_H
