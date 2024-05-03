#pragma once

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include <duckdb/common/string.hpp>
#include <duckdb/common/unordered_map.hpp>
#include <duckdb/common/unordered_set.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/common/helper.hpp>

#include <duckdb/main/client_context.hpp>
#endif 

#include "ros_value.hpp"
#include "ros_bag_types.hpp"
#include "ros_msg_types.hpp"

namespace duckdb{

const RosMsgTypes::primitive_type_map_t RosMsgTypes::FieldDef::primitive_type_map = {
    {"bool", RosValue::Type::ros_bool},
    {"int8", RosValue::Type::int8},
    {"uint8", RosValue::Type::uint8},
    {"int16", RosValue::Type::int16},
    {"uint16", RosValue::Type::uint16},
    {"int32", RosValue::Type::int32},
    {"uint32", RosValue::Type::uint32},
    {"int64", RosValue::Type::int64},
    {"uint64", RosValue::Type::uint64},
    {"float32", RosValue::Type::float32},
    {"float64", RosValue::Type::float64},
    {"string", RosValue::Type::string},
    {"time", RosValue::Type::ros_time},
    {"duration", RosValue::Type::ros_duration},

    // Deprecated types
    {"byte", RosValue::Type::int8},
    {"char", RosValue::Type::uint8},
};

class RosBagMetadata;

class RosBagReader {
public: 
    RosBagReader(ClientContext &context, RosOptions options, string file_name);
    
    shared_ptr<RosMsgTypes::MsgDef> MsgDefForTopic(const std::string &topic) const {
        const auto it = message_schemata.find(topic);
        if (it == message_schemata.end()) {
            parseMsgDefForTopic(topic);
            return message_schemata.at(topic);
        } else {
            return it->second;
        } 
    }
private:     
    void parseMsgDefForTopic(const std::string &topic) const;
}; 
}