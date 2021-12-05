//
// Created by Elena Milkai on 10/14/21.
//

#include <deltamanager/DeltaCacheData.h>

using namespace normal::htap::deltamanager;

DeltaCacheData::DeltaCacheData(std::shared_ptr<TupleSet2> delta, std::shared_ptr<int> timestamp)
        :delta_(std::move(delta)),
         timestamp_(std::move(timestamp)){}

std::shared_ptr<DeltaCacheData> DeltaCacheData::make(const std::shared_ptr<TupleSet2> &delta,
                                                            const std::shared_ptr<int> timestamp){
    return std::make_shared<DeltaCacheData>(delta, timestamp);
}

const std::shared_ptr<TupleSet2> &DeltaCacheData::getDelta() const{
    return delta_;
}

const std::shared_ptr<int> &DeltaCacheData::getTimestamp() const{
    return timestamp_;
}