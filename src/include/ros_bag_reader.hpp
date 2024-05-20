#pragma once

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include <duckdb/common/string.hpp>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/common/unordered_map.hpp>
#include <duckdb/common/unordered_set.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/multi_file_reader.hpp>
#include <duckdb/common/multi_file_reader_options.hpp>

#include <duckdb/main/client_context.hpp>
#endif 

#include "ros_value.hpp"
#include "ros_bag_types.hpp"
#include "ros_msg_types.hpp"
#include "resizable_buffer.hpp"

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
class RosBagMetadataCache;

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
    /// TODO: potentially change how column splitting works.
    bool split_header = true; 

    MultiFileReaderOptions file_options;
public:  
    void Serialize(Serializer &serializer); 
    static RosReaderOptions Deserialize(Deserializer& deserializer); 
}; 

struct RosReaderScanState {
    ResizeableBuffer repeat_buf;
    
    uint32_t current_connection_id = 0;
    size_t current_message_data_offset;
    int32_t current_message_len = 0;
}; 

class RosBagReader {
public: 
    RosBagReader(ClientContext &context, RosReaderOptions options, string file_name);
    RosBagReader(ClientContext &context, RosReaderOptions options, shared_ptr<RosBagMetadataCache> metadata);

    ~RosBagReader(); 

    const RosBagMetadata& GetMetadata() const;

    const string& Topic() const;
    const RosReaderOptions& Options() const; 

    size_t NumChunks() const; 
    size_t NumMessages() const; 

    const vector<LogicalType>& GetTypes() const;
    const vector<string>&  GetNames() const; 

    const string& GetFileName() const; 

    MultiFileReaderData             reader_data;
private: 
    struct TopicIndex {
        struct bag_offset_compare_t {
            bool operator()(const RosBagTypes::chunk_t &left, const RosBagTypes::chunk_t &right) const {
                return left.offset < right.offset;
            }
        }; 

        // For now we'll take the embag strategy of using the bag offset to sort
        // the chunks.  Potentially it would be better going forward to sort the 
        // chunks by timestamp.  No matter what this would still probably
        // be chuck write timestamp and not MESSAGE timestamp 
        std::set<const RosBagTypes::chunk_t&, bag_offset_compare_t> chunks; 
        std::unordered_set<uint32_t>                                connection_ids; 
        size_t                                                      message_cnt  = 0; 
    }; 

    shared_ptr<RosBagMetadataCache> metadata;
    shared_ptr<TopicIndex>          topic_index; 
    shared_ptr<RosMsgTypes::MsgDef> message_def; 

	RosReaderOptions                options;

    vector<LogicalType>             return_types;
	vector<string>                  names; 

    unique_ptr<FileHandle>          file_handle;
    Allocator&                      allocator; 

    void InitializeSchema(); 

    shared_ptr<RosMsgTypes::MsgDef> MakeMsgDef(const std::string &topic) const;
    shared_ptr<TopicIndex> MakeTopicIndex(const std::string &topic) const; 

}; 
}
