//
// Created by Han Cao on 5/7/21.
//

#ifndef NORMAL_DELETEMAP_H
#define NORMAL_DELETEMAP_H

namespace normal::pushdown {
    class DeleteMap {
    public:
        DeleteMap();
        void addRecord(bool toDelete);
        bool getDeleteBit (int colIndex);

    private:
        final int BITSET_SIZE = 1024;

        std::vector<std::bitset<BITSET_SIZE>>  bitsetVector;
        int currentBitIndex;
    };

}

#endif //NORMAL_DELETEMAP_H
