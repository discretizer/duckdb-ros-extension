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
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/common/serializer/deserializer.hpp>

#include <duckdb/main/client_context.hpp>
#endif 

#include "ros_value.hpp"
#include "ros_bag_types.hpp"
#include "ros_msg_types.hpp"
#include "ros_reader_options.hpp"
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
class RosBagReader {
public:
    struct ChunkIndex {
        size_t idx; 
        size_t message_cnt; 
        bool operator() (const ChunkIndex &c1, const ChunkIndex &c2){
            return (c1.idx < c2.idx); 
        }
    }; 
    using ChunkSet = set<ChunkIndex>;

    RosBagReader(ClientContext &context, RosReaderOptions options, string file_name);
    RosBagReader(ClientContext &context, RosReaderOptions options, shared_ptr<RosBagMetadataCache> metadata);

    ~RosBagReader(); 

    const RosBagMetadata& GetMetadata() const;

    const string& Topic() const;
    const RosReaderOptions& Options() const; 

    size_t NumChunks() const; 
    size_t NumMessages() const; 

    const ChunkSet& GetChunkSet() const;
    const RosBagTypes::chunk_t& GetChunk(size_t idx) const; 

    const vector<LogicalType>& GetTypes() const;
    const vector<string>&  GetNames() const; 
    const string& GetFileName() const; 

    MultiFileReaderData             reader_data;
    
    struct ScanState {
        ScanState(): 
            read_buffer{}, 
            decompression_buffer{}, 
            current_buffer{nullptr},
            chunks{}, 
            chunk_proccessed_bytes{0}
        {}

        ResizeableBuffer                            read_buffer;
        ResizeableBuffer                            decompression_buffer;
        ResizeableBuffer*                           current_buffer;

        set<idx_t>                                  chunks; 
        idx_t                                       expected_message_count;
        idx_t                                       chunk_proccessed_bytes;

        void Serialize(Serializer &serializer) const;
	    static unique_ptr<ScanState> Deserialize(Deserializer &deserializer);
    };

    void InitializeScan(RosBagReader::ScanState& scan, ChunkSet::const_iterator& current_chunk); 
    void Scan(ScanState& scan, DataChunk& result);
private: 
    struct TopicIndex {
        // For now we'll take the embag strategy of using the bag offset to sort
        // the chunks.  Potentially it would be better going forward to sort the 
        // chunks by timestamp.  No matter what this would still probably
        // be chuck write timestamp and not MESSAGE timestamp 
        ChunkSet                   chunk_set;  
        unordered_set<uint32_t>    connection_ids; 
        size_t                     message_cnt  = 0; 
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
