#pragma once 

#include "ros_msg_types.hpp"
#include <duckdb.hpp>

namespace duckdb {
LogicalType ConvertRosFieldType(const RosMsgTypes::FieldDef& def); 
}