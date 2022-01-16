//
// Created by Yifei Yang on 7/27/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_MODES_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_MODES_H

#include <memory>
#include "FullPullupMode.h"
#include "FullPushdownMode.h"
#include "PullupCachingMode.h"
#include "HybridCachingMode.h"
#include "HybridCachingLastMode.h"
#include "HtapMode.h"

namespace normal::plan::operator_::mode {

    /**
     * Mode factories
     */
    class Modes {

    public:
        static std::shared_ptr<FullPullupMode> fullPullupMode();

        static std::shared_ptr<FullPushdownMode> fullPushdownMode();

        static std::shared_ptr<PullupCachingMode> pullupCachingMode();

        static std::shared_ptr<HybridCachingMode> hybridCachingMode();

        static std::shared_ptr<HybridCachingLastMode> hybridCachingLastMode();

        static std::shared_ptr<HtapMode> htapMode();
    };

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_MODES_H
