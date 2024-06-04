#include <duckdb/common/string.hpp>
#include <duckdb/common/optional_ptr.hpp>

#include "message_parser.hpp"

namespace duckdb {

const RosValue::Pointer MessageParser::parse() {
  // The lowest number of RosValues occurs when we have a message with only doubles in a single type.
  // The number of RosValues in this case is the number of doubles that can fit in our buffer,
  // plus one for the RosValue object that will point to all the doubles.
  ros_values->reserve(message_buffer.size() / sizeof(double) + 1);
  ros_values->emplace_back(msg_def.fieldIndexes());
  ros_values_offset = 1;
  initObject(0, msg_def);
  return RosValue::Pointer(ros_values);
}

void MessageParser::initObject(size_t object_offset, const RosMsgTypes::BaseMsgDef &object_definition) {
  const size_t children_offset = ros_values_offset;
  auto& children = std::get<RosValue::object_info_t>(ros_values->at(object_offset).info_).children; 
  children.base = ros_values;
  children.offset = children_offset;
  children.length = 0;
  for (auto &member: object_definition.members()) {
    if (member.index() == 0) {
      auto& field = std::get<RosMsgTypes::FieldDef>(member);
      emplaceField(field);
    }
  }

  for (auto &member: object_definition.members()) {
    if (member.index() == 0) {
      auto& field = std::get<RosMsgTypes::FieldDef>(member);
      const size_t child_offset = children_offset + children.length++;
      switch (ros_values->at(child_offset).type_) {
        case RosValue::Type::object: {
          auto& embedded_type = field.typeDefinition();
          initObject(child_offset, embedded_type);
          break;
        }
        case RosValue::Type::array:
        case RosValue::Type::primitive_array:
        {
          initArray(child_offset, field);
          break;
        }
        default: {
          // Primitive
          initPrimitive(child_offset, field);
        }
      }
    }
  }
}

void MessageParser::emplaceField(const RosMsgTypes::FieldDef &field) {
  if (field.arraySize() == 0) {
    if (field.type() != RosValue::Type::object) {
      ros_values->emplace_back(field.type());
    } else {
      auto& object_definition = field.typeDefinition();
      ros_values->emplace_back(object_definition.fieldIndexes());
    }
  } else if (field.type() == RosValue::Type::object || field.type() == RosValue::Type::string) {
    ros_values->emplace_back(RosValue::_array_identifier());
  } else {
    ros_values->emplace_back(field.type(), field.arraySize());
  }

  ++ros_values_offset;
}

void MessageParser::initArray(size_t array_offset, const RosMsgTypes::FieldDef &field) {
  uint32_t array_length;
  if (field.arraySize() == -1) {
    message_buffer.copy(reinterpret_cast<char*>(&array_length), sizeof(uint32_t)); 
    message_buffer = message_buffer.substr(sizeof(uint32_t));
  } else {
    array_length = static_cast<uint32_t>(field.arraySize());
  }

  const RosValue::Type field_type = field.type();
  if (field_type == RosValue::Type::object || field_type == RosValue::Type::string) {
    const size_t children_offset = ros_values_offset;
    ros_values_offset += array_length;

    auto& info = std::get<RosValue::array_info_t>(ros_values->at(array_offset).info_);
    auto& children = info.children; 

    children.length = array_length;
    children.base = ros_values;
    children.offset = children_offset;

    if (field_type == RosValue::Type::string) {
      for (size_t i = 0; i < array_length; ++i) {
        ros_values->emplace_back(field.type());
      }

      for (size_t i = 0; i < array_length; ++i) {
        initPrimitive(children_offset + i, field);
      }
    } else {
      auto& object_definition = field.typeDefinition();
      for (size_t i = 0; i < array_length; ++i) {
        ros_values->emplace_back(object_definition.fieldIndexes());
      }

      for (size_t i = 0; i < array_length; ++i) {
        initObject(children_offset + i, object_definition);
      }
    }
  } else {
    auto& info = std::get<RosValue::primitive_array_info_t>(ros_values->at(array_offset).info_);
    info.length = array_length;
    info.message_buffer = message_buffer;
    message_buffer = message_buffer.substr(array_length * field.typeSize());
  }
}

void MessageParser::initPrimitive(size_t primitive_offset, const RosMsgTypes::FieldDef &field) {
  RosValue& primitive = ros_values->at(primitive_offset);
  auto& info = std::get<RosValue::primitive_info_t>(primitive.info_); 

  info.message_buffer = message_buffer;
  if (field.type() == RosValue::Type::string) {
    message_buffer = message_buffer.substr(primitive.getPrimitive<uint32_t>() + sizeof(uint32_t));
  } else {
    message_buffer = message_buffer.substr(field.typeSize());
  }
}
}