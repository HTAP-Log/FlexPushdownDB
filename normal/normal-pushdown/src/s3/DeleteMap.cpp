//
// Created by Han Cao on 5/6/21.
//

#include "../../include/normal/pushdown/s3/DeleteMap.h"
#include <bitset>
#include <vector>
#include <spdlog/spdlog.h>

/**
 * 1 represent to delete 0 represent no to delete;
 */
namespace normal::pushdown {

    const int BITSET_SIZE = 1024;

    std::vector<std::bitset<BITSET_SIZE>>  bitsetVector;
    int currentWriteBitIndex;
    int currentReadBitIndex;

    // use dynamic_bitset from boost framework
    DeleteMap::DeleteMap(long colSize) {

        for (int i = 0; i < colSize; i++) {
            bitsetVector.push_back(std::bitset<BITSET_SIZE>(0) );
        }
        currentWriteBitIndex = 0;
        currentReadBitIndex = 0;

        SPDLOG_CRITICAL("DeleteMap Constructed");
    }

    void DeleteMap::setBit(long colIndex) {
        int vecIndex = colIndex/BITSET_SIZE;
        bitsetVector[vecIndex].set(colIndex%BITSET_SIZE);
    }

    bool DeleteMap::getIfDelete() {
        int vecIndex = currentReadBitIndex/BITSET_SIZE;
        currentReadBitIndex++;
        return bitsetVector[vecIndex].test(currentReadBitIndex%BITSET_SIZE) & 1;  // can be optimize
    }

    void DeleteMap::addRecord(bool toDelete) {
        // if current long if full then create a new integer
//        if (currentBitIndexWithinLong == BITSET_SIZE) {
//            bitsetVector.push_back(std::bitset<BITSET_SIZE>);
//            currentBitIndex = 0;
//        }
//
//        if (toDelete) {
//            bitsetVector.back().set(currentBitIndex);
//            currentBitIndex++;
//        } else {
//            currentBitIndex++;
//        }
    }

    bool DeleteMap::getDeleteBit(long colIndex) {
        // decide which element in the vector
//        int vecIndex = colIndex/BITSET_SIZE;
//        return bitsetVector[vecIndex] & (1 << (colIndex%BITSET_SIZE));
        return true;
    }

}