//
// Created by Han Cao on 5/7/21.
//

#ifndef NORMAL_DELETEMAP_H
#define NORMAL_DELETEMAP_H

#include <bitset>
#include <vector>

namespace normal::pushdown {
    class DeleteMap {
    public:
        DeleteMap(long colSize);
        void addRecord(bool toDelete);
        void setBit(long colIndex);
        bool getIfDelete();
        bool getDeleteBit (long colIndex);

    private:
        static const int BITSET_SIZE = 1024;

        std::vector<std::bitset<BITSET_SIZE>>  bitsetVector;
        int currentBitIndex;
    };
}

#endif //NORMAL_DELETEMAP_H
