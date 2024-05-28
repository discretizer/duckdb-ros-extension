#pragma once 

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include <duckdb/common/multi_file_reader_options.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/common/serializer/deserializer.hpp>
#endif 

namespace duckdb {
/// @brief Options for the ROS input
/// This class should be in charge of parsing inputs for the ros reader function and
/// 
struct RosReaderOptions {
    explicit RosReaderOptions() {
    }

    /// @brief  name of the topic to extract as a table from the bag. 
    /// This is the first positional option of the command. 
    string topic;

    /// @brief Split the header field (i.e std_msgs/Header). If true the header will 
    /// be split into independent columns for each header value. 
    bool split_header = true; 

    /// @brief Include the receive timestamp in the 
    string rx_timestamp_col = "rx_timestamp"; 

    MultiFileReaderOptions file_options;
public:  
    void Serialize(Serializer &serializer); 
    static RosReaderOptions Deserialize(Deserializer& deserializer); 
};  
}