#pragma once

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include <duckdb/common/unordered_map.hpp>
#endif

#include "ros_value.hpp"
#include "ros_bag_types.hpp"
#include "ros_msg_types.hpp"
#include "span.hpp"

namespace duckdb {

class MessageParser {
 public:
  MessageParser(
    std::string_view buffer,
    const RosMsgTypes::MsgDef& msg_def
  ):  message_buffer(buffer),
      message_buffer_offset(0), 
      ros_values(make_shared<vector<RosValue>>()), 
      ros_values_offset(0), 
      msg_def(msg_def)
  {
  };

  const RosValue::Pointer parse();

 private:
  static unordered_map<string, size_t> primitive_size_map;

  void initObject(size_t object_offset, const RosMsgTypes::BaseMsgDef& object_definition);
  void initArray(size_t array_offset, const RosMsgTypes::FieldDef &field);
  void initPrimitive(size_t primitive_offset, const RosMsgTypes::FieldDef &field);
  void emplaceField(const RosMsgTypes::FieldDef &field);

  std::string_view message_buffer; 
  size_t message_buffer_offset; 

  shared_ptr<vector<RosValue> > ros_values;
  size_t ros_values_offset;

  const RosMsgTypes::MsgDef& msg_def;
};
}