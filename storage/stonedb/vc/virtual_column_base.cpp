/* Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
   Use is subject to license terms

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1335 USA
*/

#include "vc/virtual_column_base.h"

#include "core/compiled_query.h"
#include "core/mysql_expression.h"
#include "core/rc_attr.h"
#include "in_set_column.h"
#include "subselect_column.h"

namespace stonedb {
namespace vcolumn {
VirtualColumnBase::VirtualColumnBase(core::ColumnType const &ct, core::MultiIndex *mind)
    : Column(ct),
      mind(mind),
      conn_info(current_tx),
      last_val(std::make_shared<core::ValueOrNull>()),
      first_eval(true),
      dim(-1) {
  ResetLocalStatistics();
}

VirtualColumnBase::VirtualColumnBase(VirtualColumn const &vc)
    : Column(vc.ct),
      mind(vc.mind),
      conn_info(vc.conn_info),
      var_map(vc.var_map),
      params(vc.params),
      last_val(std::make_shared<core::ValueOrNull>()),
      first_eval(true),
      dim(vc.dim) {
  ResetLocalStatistics();
}

void VirtualColumnBase::MarkUsedDims(core::DimensionVector &dims_usage) {
  for (auto const &it : var_map) dims_usage[it.dim] = true;
}
std::set<int> VirtualColumnBase::GetDimensions() {
  std::set<int> d;
  for (auto const &it : var_map) d.insert(it.dim);
  return d;
}

int64_t VirtualColumnBase::NoTuples() {
  if (mind == NULL)  // constant
    return 1;
  core::DimensionVector dims(mind->NoDimensions());
  MarkUsedDims(dims);
  return mind->NoTuples(dims);
}

bool VirtualColumnBase::IsConstExpression(core::MysqlExpression *expr, int temp_table_alias,
                                          const std::vector<int> *aliases) {
  core::MysqlExpression::SetOfVars &vars = expr->GetVars();  // get all variables from complex term

  for (auto &iter : vars) {
    auto ndx_it = std::find(aliases->begin(), aliases->end(), iter.tab);
    if (ndx_it != aliases->end())
      return false;
    else if (iter.tab == temp_table_alias)
      return false;
  }
  return true;
}

void VirtualColumnBase::SetMultiIndex(core::MultiIndex *m, std::shared_ptr<core::JustATable> t) {
  mind = m;
  if (t)
    for (auto &iter : var_map) iter.tab = t;
}

void VirtualColumnBase::ResetLocalStatistics() {
  // if(Type().IsFloat()) {
  //	vc_min_val = common::MINUS_INF_64;
  //	vc_max_val = common::PLUS_INF_64;
  //} else {
  //	vc_min_val = common::MINUS_INF_64;
  //	vc_max_val = common::PLUS_INF_64;
  //}
  vc_min_val = common::NULL_VALUE_64;
  vc_max_val = common::NULL_VALUE_64;
  vc_nulls_possible = true;
  vc_dist_vals = common::NULL_VALUE_64;
  nulls_only = false;
}

void VirtualColumnBase::SetLocalMinMax(int64_t loc_min, int64_t loc_max) {
  if (loc_min == common::NULL_VALUE_64) loc_min = common::MINUS_INF_64;
  if (loc_max == common::NULL_VALUE_64) loc_max = common::PLUS_INF_64;
  if (Type().IsFloat()) {
    if (vc_min_val == common::NULL_VALUE_64 ||
        (loc_min != common::MINUS_INF_64 &&
         (*(double *)&loc_min > *(double *)&vc_min_val || vc_min_val == common::MINUS_INF_64)))
      vc_min_val = loc_min;
    if (vc_max_val == common::NULL_VALUE_64 ||
        (loc_max != common::PLUS_INF_64 &&
         (*(double *)&loc_max < *(double *)&vc_max_val || vc_max_val == common::PLUS_INF_64)))
      vc_max_val = loc_max;
  } else {
    if (vc_min_val == common::NULL_VALUE_64 || loc_min > vc_min_val) vc_min_val = loc_min;
    if (vc_max_val == common::NULL_VALUE_64 || loc_max < vc_max_val) vc_max_val = loc_max;
  }
}

int64_t VirtualColumnBase::RoughMax() {
  int64_t res = DoRoughMax();
  DEBUG_ASSERT(res != common::NULL_VALUE_64);
  if (Type().IsFloat()) {
    if (*(double *)&res > *(double *)&vc_max_val && vc_max_val != common::PLUS_INF_64 &&
        vc_max_val != common::NULL_VALUE_64)
      return vc_max_val;
  } else if (res > vc_max_val && vc_max_val != common::NULL_VALUE_64)
    return vc_max_val;
  return res;
}

int64_t VirtualColumnBase::RoughMin() {
  int64_t res = DoRoughMin();
  DEBUG_ASSERT(res != common::NULL_VALUE_64);
  if (Type().IsFloat()) {
    if (*(double *)&res < *(double *)&vc_min_val && vc_min_val != common::MINUS_INF_64 &&
        vc_min_val != common::NULL_VALUE_64)
      return vc_min_val;
  } else if (res < vc_min_val && vc_min_val != common::NULL_VALUE_64)
    return vc_min_val;
  return res;
}

int64_t VirtualColumnBase::GetApproxDistVals(bool incl_nulls, core::RoughMultiIndex *rough_mind) {
  int64_t res = DoGetApproxDistVals(incl_nulls, rough_mind);
  if (vc_dist_vals != common::NULL_VALUE_64) {
    int64_t local_res = vc_dist_vals;
    if (incl_nulls && NullsPossible()) local_res++;
    if (res == common::NULL_VALUE_64 || res > local_res) res = local_res;
  }
  if (!Type().IsFloat() && vc_min_val > (common::MINUS_INF_64 / 3) && vc_max_val < (common::PLUS_INF_64 / 3)) {
    int64_t local_res = vc_max_val - vc_min_val + 1;
    if (incl_nulls && NullsPossible()) local_res++;
    if (res == common::NULL_VALUE_64 || res > local_res) return local_res;
  }
  if (Type().IsFloat() && vc_min_val != common::NULL_VALUE_64 && vc_min_val == vc_max_val) {
    int64_t local_res = 1;
    if (incl_nulls && NullsPossible()) local_res++;
    if (res == common::NULL_VALUE_64 || res > local_res) return local_res;
  }
  return res;
}

int64_t VirtualColumnBase::GetMaxInt64(const core::MIIterator &mit) {
  int64_t res = DoGetMaxInt64(mit);
  DEBUG_ASSERT(res != common::NULL_VALUE_64);
  if (Type().IsFloat()) {
    if (*(double *)&res > *(double *)&vc_max_val && vc_max_val != common::PLUS_INF_64 &&
        vc_max_val != common::NULL_VALUE_64)
      return vc_max_val;
  } else if ((vc_max_val != common::NULL_VALUE_64 && res > vc_max_val))
    return vc_max_val;
  return res;
}

int64_t VirtualColumnBase::GetMinInt64(const core::MIIterator &mit) {
  int64_t res = DoGetMinInt64(mit);
  DEBUG_ASSERT(res != common::NULL_VALUE_64);
  if (Type().IsFloat()) {
    if (*(double *)&res < *(double *)&vc_min_val && vc_min_val != common::MINUS_INF_64 &&
        vc_min_val != common::NULL_VALUE_64)
      return vc_min_val;
  } else if ((vc_min_val != common::NULL_VALUE_64 && res < vc_min_val))
    return vc_min_val;
  return res;
}

int64_t VirtualColumnBase::DoGetApproxSum(const core::MIIterator &mit, bool &nonnegative) {
  int64_t res = DoGetSum(mit, nonnegative);
  if (res != common::NULL_VALUE_64) return res;
  res = DoGetMaxInt64(mit);
  int64_t n = mit.GetPackSizeLeft();
  if (res == common::PLUS_INF_64 || n == common::NULL_VALUE_64) return common::NULL_VALUE_64;
  if (Type().IsFloat()) {
    double d = *(double *)&res;
    if (d <= 0) return 0;
    d *= n;
    if (d > -9.223372037e+18 && d < 9.223372037e+18) return *(int64_t *)(&d);
  } else {
    if (res <= 0) return 0;
    if (res < 0x00007FFFFFFFFFFFll && n <= (1 << mit.GetPower())) return n * res;
  }
  return common::NULL_VALUE_64;
}

int64_t VirtualColumnBase::DecodeValueAsDouble(int64_t code) {
  if (Type().IsFloat()) return code;  // no conversion
  double res = double(code) / types::PowOfTen(ct.GetScale());
  return *(int64_t *)(&res);
}

common::RSValue VirtualColumnBase::DoRoughCheck(const core::MIIterator &mit, core::Descriptor &d) {
  // default implementation
  if (d.op == common::O_FALSE) return common::RS_NONE;
  if (d.op == common::O_TRUE) return common::RS_ALL;
  bool nulls_possible = NullsPossible();
  if (d.op == common::O_IS_NULL || d.op == common::O_NOT_NULL) {
    if (GetNoNulls(mit) == mit.GetPackSizeLeft()) {  // nulls only
      if (d.op == common::O_IS_NULL)
        return common::RS_ALL;
      else
        return common::RS_NONE;
    }
    if (!nulls_possible) {
      if (d.op == common::O_IS_NULL)
        return common::RS_NONE;
      else
        return common::RS_ALL;
    }
    return common::RS_SOME;
  }
  common::RSValue res = common::RS_SOME;
  if (d.val1.vc == NULL || ((d.op == common::O_BETWEEN || d.op == common::O_NOT_BETWEEN) && d.val2.vc == NULL))
    return common::RS_SOME;  // irregular descriptor - cannot use VirtualColumn
                             // rough statistics
  // In all other situations: common::RS_NONE for nulls only
  if (GetNoNulls(mit) == mit.GetPackSizeLeft() ||
      (!d.val1.vc->IsMultival() &&
       (d.val1.vc->GetNoNulls(mit) == mit.GetPackSizeLeft() ||
        (d.val2.vc && !d.val2.vc->IsMultival() && d.val2.vc->GetNoNulls(mit) == mit.GetPackSizeLeft()))))
    return common::RS_NONE;
  if (d.op == common::O_LIKE || d.op == common::O_NOT_LIKE || d.val1.vc->IsMultival() ||
      (d.val2.vc && d.val2.vc->IsMultival()))
    return common::RS_SOME;

  if (Type().IsString()) {
    if (types::RequiresUTFConversions(d.GetCollation())) return common::RS_SOME;
    types::BString vamin = GetMinString(mit);
    types::BString vamax = GetMaxString(mit);
    types::BString v1min = d.val1.vc->GetMinString(mit);
    types::BString v1max = d.val1.vc->GetMaxString(mit);
    types::BString v2min = (d.val2.vc ? d.val2.vc->GetMinString(mit) : types::BString());
    types::BString v2max = (d.val2.vc ? d.val2.vc->GetMaxString(mit) : types::BString());
    if (vamin.IsNull() || vamax.IsNull() || v1min.IsNull() || v1max.IsNull() ||
        (d.val2.vc && (v2min.IsNull() || v2max.IsNull())))
      return common::RS_SOME;
    // Note: rough string values are not suitable to ensuring equality. Only
    // inequalities are processed.
    if (d.op == common::O_BETWEEN || d.op == common::O_NOT_BETWEEN) {  // the second case will be negated soon
      if (vamin > v1max && vamax < v2min)
        res = common::RS_ALL;
      else if (vamin > v2max || vamax < v1min)
        res = common::RS_NONE;
    } else {
      if (vamin > v1max) {  // NOTE: only common::O_MORE, common::O_LESS and
                            // common::O_EQ are analyzed here, the rest of operators
                            // will be taken into account later (treated as negations)
        if (d.op == common::O_EQ || d.op == common::O_NOT_EQ || d.op == common::O_LESS || d.op == common::O_MORE_EQ)
          res = common::RS_NONE;
        if (d.op == common::O_MORE || d.op == common::O_LESS_EQ) res = common::RS_ALL;
      }
      if (vamax < v1min) {
        if (d.op == common::O_EQ || d.op == common::O_NOT_EQ || d.op == common::O_MORE || d.op == common::O_LESS_EQ)
          res = common::RS_NONE;
        if (d.op == common::O_LESS || d.op == common::O_MORE_EQ) res = common::RS_ALL;
      }
    }
  } else {
    if (!Type().IsNumComparable(d.val1.vc->Type()) ||
        ((d.op == common::O_BETWEEN || d.op == common::O_NOT_BETWEEN) && !Type().IsNumComparable(d.val2.vc->Type())))
      return common::RS_SOME;  // non-numerical or non-comparable

    int64_t vamin = GetMinInt64(mit);
    int64_t vamax = GetMaxInt64(mit);
    int64_t v1min = d.val1.vc->GetMinInt64(mit);
    int64_t v1max = d.val1.vc->GetMaxInt64(mit);
    int64_t v2min = (d.val2.vc ? d.val2.vc->GetMinInt64(mit) : common::NULL_VALUE_64);
    int64_t v2max = (d.val2.vc ? d.val2.vc->GetMaxInt64(mit) : common::NULL_VALUE_64);
    if (vamin == common::NULL_VALUE_64 || vamax == common::NULL_VALUE_64 || v1min == common::NULL_VALUE_64 ||
        v1max == common::NULL_VALUE_64 ||
        (d.val2.vc && (v2min == common::NULL_VALUE_64 || v2max == common::NULL_VALUE_64)))
      return common::RS_SOME;

    if (!Type().IsFloat()) {
      if (d.op == common::O_BETWEEN || d.op == common::O_NOT_BETWEEN) {  // the second case will be negated soon
        if (vamin >= v1max && vamax <= v2min)
          res = common::RS_ALL;
        else if (vamin > v2max || vamax < v1min)
          res = common::RS_NONE;
      } else {
        if (vamin >= v1max) {  // NOTE: only common::O_MORE, common::O_LESS and
                               // common::O_EQ are analyzed here, the rest of operators
                               // will be taken into account later (treated as negations)
          if (d.op == common::O_LESS || d.op == common::O_MORE_EQ)  // the second case will be negated soon
            res = common::RS_NONE;
          if (vamin > v1max) {
            if (d.op == common::O_EQ || d.op == common::O_NOT_EQ) res = common::RS_NONE;
            if (d.op == common::O_MORE || d.op == common::O_LESS_EQ) res = common::RS_ALL;
          }
        }
        if (vamax <= v1min) {
          if (d.op == common::O_MORE || d.op == common::O_LESS_EQ)  // the second case will be negated soon
            res = common::RS_NONE;
          if (vamax < v1min) {
            if (d.op == common::O_EQ || d.op == common::O_NOT_EQ) res = common::RS_NONE;
            if (d.op == common::O_LESS || d.op == common::O_MORE_EQ) res = common::RS_ALL;
          }
        }
        if (res == common::RS_SOME && (d.op == common::O_EQ || d.op == common::O_NOT_EQ) && vamin == v1max &&
            vamax == v1min)
          res = common::RS_ALL;
      }
    } else {
      double vamind = (vamin == common::MINUS_INF_64 ? common::MINUS_INF_DBL : *(double *)&vamin);
      double vamaxd = (vamax == common::PLUS_INF_64 ? common::PLUS_INF_DBL : *(double *)&vamax);
      double v1mind = (v1min == common::MINUS_INF_64 ? common::MINUS_INF_DBL : *(double *)&v1min);
      double v1maxd = (v1max == common::PLUS_INF_64 ? common::PLUS_INF_DBL : *(double *)&v1max);
      double v2mind = (v2min == common::MINUS_INF_64 ? common::MINUS_INF_DBL : *(double *)&v2min);
      double v2maxd = (v2max == common::PLUS_INF_64 ? common::PLUS_INF_DBL : *(double *)&v2max);

      if (d.op == common::O_BETWEEN || d.op == common::O_NOT_BETWEEN) {  // the second case will be negated soon
        if (vamind >= v1maxd && vamaxd <= v2mind)
          res = common::RS_ALL;
        else if (vamind > v2maxd || vamaxd < v1mind)
          res = common::RS_NONE;
      } else {
        if (vamind >= v1maxd) {                                     // NOTE: only common::O_MORE, common::O_LESS
                                                                    // and common::O_EQ are analyzed here, the rest
                                                                    // of operators will be taken into account
                                                                    // later (treated as negations)
          if (d.op == common::O_LESS || d.op == common::O_MORE_EQ)  // the second case will be negated soon
            res = common::RS_NONE;
          if (vamind > v1maxd) {
            if (d.op == common::O_EQ || d.op == common::O_NOT_EQ) res = common::RS_NONE;
            if (d.op == common::O_MORE || d.op == common::O_LESS_EQ) res = common::RS_ALL;
          }
        }
        if (vamaxd <= v1mind) {
          if (d.op == common::O_MORE || d.op == common::O_LESS_EQ)  // the second case will be negated soon
            res = common::RS_NONE;
          if (vamaxd < v1mind) {
            if (d.op == common::O_EQ || d.op == common::O_NOT_EQ) res = common::RS_NONE;
            if (d.op == common::O_LESS || d.op == common::O_MORE_EQ) res = common::RS_ALL;
          }
        }
        if (res == common::RS_SOME && (d.op == common::O_EQ || d.op == common::O_NOT_EQ) && vamind == v1maxd &&
            vamaxd == v1mind)
          res = common::RS_ALL;
      }
    }
  }
  // reverse negations
  if (d.op == common::O_NOT_EQ || d.op == common::O_LESS_EQ || d.op == common::O_MORE_EQ ||
      d.op == common::O_NOT_BETWEEN) {
    if (res == common::RS_ALL)
      res = common::RS_NONE;
    else if (res == common::RS_NONE)
      res = common::RS_ALL;
  }
  // check nulls
  if (res == common::RS_ALL &&
      (nulls_possible || d.val1.vc->NullsPossible() || (d.val2.vc && d.val2.vc->NullsPossible())))
    res = common::RS_SOME;
  return res;
}
}  // namespace vcolumn
}  // namespace stonedb