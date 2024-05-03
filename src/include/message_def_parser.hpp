#pragma once
#include "ros_msg_types.hpp"

namespace duckdb {
    shared_ptr<RosMsgTypes::MsgDef> parseMsgDef(string &def, const string& name);
}