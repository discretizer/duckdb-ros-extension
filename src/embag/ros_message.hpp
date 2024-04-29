#pragma once

#include <string>

#include "ros_value.hpp"
#include "message_parser.hpp"
#include "ros_msg_types.hpp"
#include "span.hpp"
#include "util.hpp"

namespace Embag {
class RosMessage {
 public:
  std::string topic;
  RosValue::ros_time_t timestamp;
  std::string md5;
  const std::shared_ptr<std::vector<char>> raw_buffer;
  const size_t raw_buffer_offset;
  uint32_t raw_data_len = 0;

  RosMessage(const std::shared_ptr<std::vector<char>> raw_buffer, const size_t offset)
    : raw_buffer(raw_buffer)
    , raw_buffer_offset(offset)
  {
  }

  RosMessage(
    const std::string& topic,
    const RosValue::ros_time_t& timestamp,
    const std::string& md5,
    const std::shared_ptr<std::vector<char>>& raw_buffer,
    size_t offset,
    uint32_t raw_data_len,
    const std::shared_ptr<RosMsgTypes::MsgDef>& msg_def
  )
    : topic(topic)
    , timestamp(timestamp)
    , md5(md5)
    , raw_buffer(raw_buffer)
    , raw_buffer_offset(offset)
    , raw_data_len(raw_data_len)
    , msg_def_(msg_def)
  {
  }

  const RosValue::Pointer &data() {
    if (!parsed_) {
      hydrate();
    }

    return data_;
  }

  bool has(const std::string &key) {
    if (!parsed_) {
      hydrate();
    }

    return data_->has(key);
  }

  void print() {
    if (!parsed_) {
      hydrate();
    }

    data_->print();
  }

  std::string getTypeName(){
    return msg_def_->name();
  }

  std::string toString() {
    if (!parsed_) {
      hydrate();
    }

    return data_->toString();
  }

 private:
  bool parsed_ = false;
  RosValue::Pointer data_;
  std::shared_ptr<RosMsgTypes::MsgDef> msg_def_;

  void hydrate() {
    MessageParser msg(raw_buffer, raw_buffer_offset, *msg_def_);

    data_ = msg.parse();

    parsed_ = true;
  }

  friend class View;
};
}