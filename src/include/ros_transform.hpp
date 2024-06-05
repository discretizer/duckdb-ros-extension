#pragma once 

#include "ros_msg_types.hpp"
#include "ros_reader_options.hpp"

#include <duckdb.hpp>

namespace duckdb {
struct RosTransform {
    
    /// @brief
    static LogicalType ConvertRosFieldType(const RosMsgTypes::FieldDef& def); 
    static bool TransformMessages(const RosReaderOptions& options, 
                                  const vector<RosValue::Pointer>& messages, 
                                  const vector<RosValue::ros_time_t>& message_rx_time, 
                                  const vector<string>& col_names,
                                  vector<Vector>& result); 
    static bool TransformTypeToSchema(const RosMsgTypes::MsgDef& msg_def, 
                                            RosReaderOptions& options, 
                                            vector<string>& names, 
                                            vector<LogicalType>& types);
}; 
}