//
// Created by matt on 22/9/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SCANOPERATOR_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SCANOPERATOR_H

#include <caf/all.hpp>
#include <normal/core/Globals.h>

CAF_BEGIN_TYPE_ID_BLOCK(ScanOperator, normal::core::ScanOperator_first_custom_type_id)
CAF_ADD_ATOM(ScanOperator, ScanAtom)
CAF_END_TYPE_ID_BLOCK(ScanOperator)

namespace normal::pushdown {

//using ScanAtom = caf::atom_constant<caf::atom("scan")>;

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SCANOPERATOR_H
