#pragma once

#include "ros_msg_types.hpp"

namespace Embag {

std::shared_ptr<RosMsgTypes::MsgDef> parseMsgDef(const std::string &def, const std::string& name);

}