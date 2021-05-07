//
// Created by Han Cao on 5/7/21.
//

#include "../../include/normal/pushdown/s3/DeleteMap.h"

/**
 * 1 represent to delete 0 represent no to delete;
 */
namespace normal::pushdown {

    final int BITSET_SIZE = 1024;

    std::vector<std::bitset<BITSET_SIZE>>  bitsetVector;
    int currentBitIndex;

    DeleteMap::DeleteMap() {
        bitsetVector.pushback(std::bitset<BITSET_SIZE>);
        currentBitIndex = 0;
    }

    void DeleteMap::addRecord(bool toDelete) {
        // if current long if full then create a new integer
        if (currentBitIndexWithinLong == BITSET_SIZE) {
            bitsetVector.pushback(std::bitset<BITSET_SIZE>);
            currentBitIndex = 0;
        }

        if (toDelete) {
            bitsetVector.back().set(currentBitIndex);
            currentBitIndex++;
        } else {
            currentBitIndex++;
        }
    }

    bool DeleteMap::getDeleteBit(long colIndex) {
        // decide which element in the vector
        int vecIndex = colIndex/BITSET_SIZE;
        return bitsetVector[vecIndex] & (1 << (colIndex%BITSET_SIZE));
    }

}