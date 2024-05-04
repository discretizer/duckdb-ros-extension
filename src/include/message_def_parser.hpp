#pragma once
#include "ros_msg_types.hpp"

namespace duckdb {
    shared_ptr<RosMsgTypes::MsgDef> ParseMsgDef(const string &def, const string& name);
}