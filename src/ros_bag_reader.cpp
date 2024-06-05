#include "ros_bag_reader.hpp"
#include "ros_bag_metadata.hpp"
#include "record_parser.hpp"
#include "message_def_parser.hpp"
#include "ros_transform.hpp"
#include "message_parser.hpp"

#include "duckdb/common/shared_ptr.hpp"

#include <string_view>
#include <array>

namespace duckdb {

using namespace std::literals::string_view_literals;

constexpr auto ROSBAG_MAGIC_STRING = "#ROSBAG V"sv; 

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

/// @brief Reads the appropriate metadata from the given bag.  
/// @param allocator 
/// @param file_handle 
/// @return 
static shared_ptr<RosBagMetadataCache>
LoadMetadata(Allocator &allocator, FileHandle &file_handle) {
    auto current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    auto metadata = make_uniq<RosBagMetadata>();

    std::array<char, ROSBAG_MAGIC_STRING.size()> temp; 

    // First, check for the magic string indicating this is indeed a bag file

    file_handle.Read(temp.data(), ROSBAG_MAGIC_STRING.size());

    if (std::string_view(temp.data(), ROSBAG_MAGIC_STRING.size() ) != ROSBAG_MAGIC_STRING) {
        throw std::runtime_error("This file doesn't appear to be a bag file...");
    }

    // Next, parse the version
    file_handle.Read(temp.data(), 3);

    // Only version 2.0 is supported at the moment
    if (std::string_view(temp.data(), 3) != "2.0") {
        throw std::runtime_error("Unsupported bag file version: " + std::string(temp.data(), 3) );
    }

    // The version is followed by a newline
    file_handle.Read(temp.data(), 1);
    if (std::string_view(temp.data(), 1) != "\n") {
        throw std::runtime_error("Unable to find newline after version string, perhaps this bag file is corrupted?");
    }

    // Docs on the RosBag 2.0 format: http://wiki.ros.org/Bags/Format/2.0
    /**
    * Read the BAG_HEADER record. So long as the file is not corrupted, this is guaranteed
    * to be the first record in the bag
    */
    uint32_t connection_count;
    uint32_t chunk_count;
    uint64_t index_pos;

    RosRecordParser record_parser(allocator); 
    record_parser.Read(file_handle, true);

    readFields(record_parser.Header(), 
        field("conn_count", connection_count), 
        field("chunk_count", chunk_count ), 
        field("index_pos", index_pos)
    ); 
    
    metadata->connections.resize(connection_count); 
    metadata->chunk_infos.resize(chunk_count); 
    metadata->chunks.reserve(chunk_count); 

    /**
     * Read connection records. 
    */
    file_handle.Seek(index_pos); 
    for (idx_t i = 0; i < connection_count; i++) {
        record_parser.Read(file_handle); 

        uint32_t connection_id; 
        std::string topic; 

        readFields(record_parser.Header(), 
            field("conn", connection_id), 
            field("topic", topic)
        ); 
        if (topic.empty()) {
            continue;
        }
        RosBagTypes::connection_data_t connection_data;
        connection_data.topic = topic;

        std::string latching_str = ""; 
        readFields(record_parser.Data(), 
            field("type", connection_data.type), 
            field("md5sum", connection_data.md5sum), 
            field("message_definition", connection_data.message_definition), 
            field("callerid", connection_data.callerid ), 
            field("latching", latching_str)
        ); 
        connection_data.latching = (latching_str == "1"); 
        const size_t slash_pos = connection_data.type.find_first_of('/');
        if (slash_pos != std::string::npos) {
            connection_data.scope = connection_data.type.substr(0, slash_pos);
        }
        metadata->connections[connection_id].id = connection_id;
        metadata->connections[connection_id].topic = topic;
        metadata->connections[connection_id].data = connection_data;
        metadata->topic_connection_map[topic].emplace_back(connection_id);
    }
    /**
    * Read the CHUNK_INFO records. These are guaranteed to be immediately after the CONNECTION records,
    * so no need to seek the file pointer
    */
    for (size_t i = 0; i < chunk_count; i++) {
        record_parser.Read(file_handle, true); 
        RosBagTypes::chunk_info_t chunk_info;

        uint32_t ver;
        readFields(record_parser.Header(), 
            field("ver", ver), 
            field("chunk_pos", chunk_info.chunk_pos), 
            field("start_time", chunk_info.start_time), 
            field("end_time", chunk_info.end_time),
            field("count", chunk_info.connection_count) 
        ); 
        metadata->chunk_infos[i] = chunk_info;
    }
    /**
     * Now that we have some chunk metadata from the CHUNK_INFO records, process the CHUNK records from
     * earlier in the file. Each CHUNK_INFO knows the position of its corresponding chunk.
     */
    for (size_t i = 0; i < chunk_count; i++) {
        auto& info = metadata->chunk_infos[i];
        RosBagTypes::chunk_t chunk; 

        chunk.offset = info.chunk_pos; 
        // TODO: The chunk infos are not necessarily
        // Revisit this logic if seeking back and forth across the file causes a slowdown
       
        file_handle.Seek(info.chunk_pos); 
        record_parser.Read(file_handle, true); 
        
        readFields(record_parser.Header(), 
            field("compression", chunk.compression), 
            field("size", chunk.uncompressed_size)
        );  
        chunk.header_len = record_parser.GetHeaderLength(); 
        chunk.data_len = record_parser.GetDataLength(); 

        if (!(chunk.compression == "lz4" || chunk.compression == "bz2" || chunk.compression == "none")) {
            throw std::runtime_error("Unsupported compression type: " + chunk.compression);
        }

        // Each chunk is followed by multiple INDEX_DATA records, so parse those out here
        for (size_t j = 0; j < info.connection_count; j++) {
            // TODO: An INDEX_DATA record contains each message's timestamp and offset within the chunk.
            // We currently don't save this information, but we potentially could to speed up accesses
            // that only want data after a certain time.

            record_parser.Read(file_handle, true); 

            uint32_t version;
            uint32_t connection_id;
            uint32_t msg_count;
      
            readFields(  record_parser.Header(), 
                field("ver", version), 
                field("conn", connection_id), 
                field("count", msg_count)
            ); 

            metadata->connections[connection_id].blocks.emplace_back(i, msg_count); 
            metadata->connections[connection_id].data.message_count += msg_count; 
        }
        chunk.info = info;
        metadata->chunks.push_back(chunk);
    }
    return make_shared_ptr<RosBagMetadataCache>(std::move(metadata), current_time);
}

RosBagReader::RosBagReader(ClientContext& context, RosReaderOptions ros_options, string file_name):
    options(std::move(ros_options)), allocator(BufferAllocator::Get(context))
{
	file_handle = FileSystem::GetFileSystem(context).OpenFile(file_name, FileOpenFlags::FILE_FLAGS_READ);
	if (!file_handle->CanSeek()) {
		throw NotImplementedException(
		    "Reading ros bags files from a FIFO stream is not supported and cannot be efficiently supported since "
		    "metadata is located at the end of the file. Write the stream to disk first and read from there instead.");
	}
	// If object cached is disabled
	// or if this file has cached metadata
	// or if the cached version already expired
	if (!ObjectCache::ObjectCacheEnabled(context)) {
		metadata = LoadMetadata(allocator, *file_handle);
	} else {
		auto last_modify_time = FileSystem::GetFileSystem(context).GetLastModifiedTime(*file_handle);
		metadata = ObjectCache::GetObjectCache(context).Get<RosBagMetadataCache>(file_name);
		if (!metadata || (last_modify_time + 10 >= metadata->read_time)) {
			metadata = LoadMetadata(allocator, *file_handle);
			ObjectCache::GetObjectCache(context).Put(file_name, metadata);
		}
	}
    if (options.topic != "") {
        InitializeSchema();
        topic_index = MakeTopicIndex(options.topic);  
    }
}

RosBagReader::RosBagReader(ClientContext &context, RosReaderOptions options, shared_ptr<RosBagMetadataCache> metadata): 
     metadata(std::move(metadata)), options(std::move(options)), allocator(BufferAllocator::Get(context))
{
    if (options.topic != "") {
        InitializeSchema();
        topic_index = MakeTopicIndex(options.topic);  
    }
}

RosBagReader::~RosBagReader() {
}


const RosBagMetadata& RosBagReader::GetMetadata() const {
    return *(metadata->metadata);
}

const string& RosBagReader::Topic() const {
    return options.topic; 
}

const RosReaderOptions& RosBagReader::Options() const {
    return options; 
}

size_t RosBagReader::NumTopicChunks() const {
    return topic_index->chunk_set.size(); 
} 

size_t RosBagReader::NumTopicMessages() const {
    return topic_index->message_cnt; 
}

const RosBagReader::ChunkSet& RosBagReader::GetTopicChunkSet() const {
    return topic_index->chunk_set; 
}

const RosBagTypes::chunk_t& RosBagReader::GetChunk(size_t index) const {
    return metadata->metadata->chunks[index]; 
}
const vector<LogicalType>& RosBagReader::GetTypes() const {
    return return_types; 
}

const vector<string>&  RosBagReader::GetNames() const {
    return names; 
}

string RosBagReader::GetFileName() const {
    return file_handle->GetPath(); 
}

void RosBagReader::ScanState::Serialize(Serializer &serializer) const {
    serializer.WriteProperty<decltype(ScanState::chunks)>(100, "chucks", chunks);
	serializer.WriteProperty<idx_t>(101, "processed_bytes", chunk_proccessed_bytes);
}

unique_ptr<RosBagReader::ScanState> RosBagReader::ScanState::Deserialize(Deserializer &deserializer) {
	auto chunks = deserializer.ReadProperty<decltype(ScanState::chunks)>(100, "chucks");
	auto processed_byte = deserializer.ReadProperty<idx_t>(101, "processed_bytes");

    auto scan_state = make_uniq<RosBagReader::ScanState>(); 
    scan_state->chunks = chunks;
    scan_state->chunk_proccessed_bytes = processed_byte;

    return scan_state; 
}

void RosBagReader::Scan(RosBagReader::ScanState& scan_state, DataChunk& result) {
    if (scan_state.chunks.empty()) {
        return; 
    }

    vector<RosValue::Pointer> message_values;
    vector<RosValue::ros_time_t> message_rx_time; 

    message_values.reserve(scan_state.expected_message_count); 
    message_rx_time.reserve(scan_state.expected_message_count); 
    
    auto front = scan_state.chunks.begin();
    auto chunk_idx = *front;
    scan_state.chunks.erase(front);

    auto chunk = GetChunk(chunk_idx); 

    // If we don't have a current buffer, lets create one and decompress the data into it if 
    // neccessairy
    
    if ((scan_state.current_buffer == nullptr) || 
        (scan_state.current_buffer->len == 0)) {
        constexpr size_t LENGTH_OFFSET = 8U; // Offsets of length fields: 4 bytes + 4bytes;
        scan_state.read_buffer.resize(allocator, chunk.data_len); 
        file_handle->Read(scan_state.read_buffer.ptr, chunk.data_len, chunk.offset + chunk.header_len + LENGTH_OFFSET); 
        if (chunk.compression != "none") {
            scan_state.decompression_buffer.resize(allocator, chunk.uncompressed_size); 
            chunk.decompress(scan_state.read_buffer.ptr, scan_state.decompression_buffer.ptr);
            scan_state.read_buffer.inc(chunk.data_len);  
            scan_state.current_buffer = &scan_state.decompression_buffer; 
        } else {
            scan_state.current_buffer = &scan_state.read_buffer; 
        }
    }
    
    // Parse through the buffer reading each record 
    // because we're only parsing one topic - a future optimization 
    // could be to generate a message index for each topic. 
    // For now see the performace we get by just looping quickly through all 
    // the messages; 
    
    bool once = true; 
    //while (scan_state.current_buffer->len != 0) {
    while (once) {
   
        //RosBufferedRecordParser record(*(scan_state.current_buffer)); 
        uint8_t op = 7; 
        uint32_t conn_id; 
        RosValue::ros_time_t timestamp; 
        once = false; 
        scan_state.current_buffer->inc(scan_state.current_buffer->len);
        /*
        readFields( record.Header(),
            field("op", op), 
            field("conn", conn_id),
            field("time", timestamp) 
        );
        */
        switch (RosBagTypes::op(op)) {
            case RosBagTypes::op::MESSAGE_DATA: {
                /*
                if (topic_index->connection_ids.count(conn_id) == 0) {
                    continue;
                }
                MessageParser parser(record.Data(), *message_def); 

                message_values.emplace_back(parser.parse()); 
                message_rx_time.emplace_back(timestamp); 
                */
    
                continue;
            }
            case RosBagTypes::op::CONNECTION: {
                continue;
            }
            default: {
                throw std::runtime_error("Found unknown record type: " + std::to_string(static_cast<int>(op)));
            }
        }
    }

    //RosTransform::TransformMessages(options, message_values, message_rx_time, names, result.data); 
    //result.SetCardinality(MinValue(message_values.size(), size_t(STANDARD_VECTOR_SIZE))); 
}

void RosBagReader::InitializeScan(RosBagReader::ScanState& scan, std::optional<RosBagReader::ChunkSet::const_iterator>& current_chunk) {
    scan.expected_message_count = 0;  
    if (!current_chunk.has_value()) {
        current_chunk = GetTopicChunkSet().begin(); 
    }
    
    scan.chunks.emplace_hint(scan.chunks.cend(), current_chunk.value()->idx); 
    scan.expected_message_count += current_chunk.value()->message_cnt; 
    current_chunk.value()++; 
}


void RosBagReader::InitializeSchema() {
    message_def = MakeMsgDef(options.topic); 
    RosTransform::TransformTypeToSchema(*message_def, options, names, return_types );
}

shared_ptr<RosMsgTypes::MsgDef> RosBagReader::MakeMsgDef(const string &topic) const {
    const auto conn_data = GetMetadata().getConnectionData(topic); 
    return ParseMsgDef(conn_data.message_definition, conn_data.type); 
}

shared_ptr<RosBagReader::TopicIndex> RosBagReader::MakeTopicIndex(const std::string &topic) const {
    auto topic_idx = make_shared_ptr<RosBagReader::TopicIndex>(); 

    for (const auto &connection_id : metadata->metadata->topic_connection_map.at(topic)) {
        for (const auto& block: GetMetadata().connections[connection_id].blocks) {
            topic_idx->message_cnt += block.message_count; 
            topic_idx->chunk_set.emplace(block.chunk_idx, block.message_count);
        }
        topic_idx->connection_ids.emplace(connection_id);
    }
    return topic_idx; 
}   
}

 