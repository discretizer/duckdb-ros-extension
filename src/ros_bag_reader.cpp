#include "ros_bag_reader.hpp"
#include "ros_bag_metadata.hpp"
#include "record_parser.hpp"
#include "message_def_parser.hpp"
#include "ros_schema.hpp"

#include <string_view>
#include <array>

namespace duckdb {

using namespace std::literals::string_view_literals;

constexpr auto ROSBAG_MAGIC_STRING = "#ROSBAG V"sv; 

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
        make_field("conn_count", connection_count), 
        make_field("chunck_count", chunk_count ), 
        make_field("index_pos", index_pos)
    ); 
    
    metadata->connections.resize(connection_count); 
    metadata->chunk_infos.reserve(chunk_count); 
    metadata->chunks.reserve(chunk_count); 

    file_handle.Seek(index_pos); 
    for (idx_t i = 0; i < connection_count; i++) {
        record_parser.Read(file_handle); 

        uint32_t connection_id; 
        std::string topic; 

        readFields(record_parser.Header(), 
            make_field("conn", connection_id), 
            make_field("topic", topic)
        ); 
        if (topic.empty()) {
            continue;
        }
        RosBagTypes::connection_data_t connection_data;
        connection_data.topic = topic;

        std::string latching_str; 
        readFields(record_parser.Data(), 
            make_field("type", connection_data.type), 
            make_field("md5sum", connection_data.md5sum), 
            make_field("message_definition", connection_data.message_definition), 
            make_field("callerid", connection_data.callerid ), 
            make_field("latching", latching_str)
        ); 
        connection_data.latching = (latching_str == "1"); 
        const size_t slash_pos = connection_data.type.find_first_of('/');
        if (slash_pos != std::string::npos) {
            connection_data.scope = connection_data.type.substr(0, slash_pos);
        }
        metadata->connections[connection_id].id = connection_id;
        metadata->connections[connection_id].topic = topic;
        metadata->connections[connection_id].data = connection_data;
        metadata->topic_connection_map[topic].push_back(metadata->connections[connection_id]);
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
            make_field("ver", ver), 
            make_field("chunk_pos", chunk_info.chunk_pos), 
            make_field("start_time", chunk_info.start_time), 
            make_field("end_time", chunk_info.end_time),
            make_field("count", chunk_info.connection_count) 
        ); 
        metadata->chunk_infos[i] = chunk_info;
    }
    /**
     * Now that we have some chunk metadata from the CHUNK_INFO records, process the CHUNK records from
     * earlier in the file. Each CHUNK_INFO knows the position of its corresponding chunk.
     */
    for (size_t i = 0; i < chunk_count; i++) {
        auto& info = metadata->chunk_infos[i];

        // TODO: The chunk infos are not necessarily Revisit this logic if seeking back and forth across the file causes a slowdown
        file_handle.Seek(info.chunk_pos); 
        record_parser.Read(file_handle, true); 
        RosBagTypes::chunk_t chunk; 

        chunk.offset = file_handle.SeekPosition(); 
        readFields(record_parser.Header(), 
            make_field("compression", chunk.compression), 
            make_field("size", chunk.uncompressed_size)
        );  

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
                make_field("ver", version), 
                make_field("conn", connection_id), 
                make_field("count", msg_count)
            ); 
            // NOTE: It seems like it would be simpler to just do &chunk here right? WRONG.
            //       C++ reuses the same memory location for the chunk variable for each loop, so
            //       if you use &chunk, all `into_chunk` values will be exactly the same
            metadata->connections[connection_id].blocks.push_back(metadata->chunks.at(i)); 
            metadata->connections[connection_id].data.message_count += msg_count; 
        }
        chunk.info = info;
        metadata->chunks.push_back(chunk);
    }
    return make_shared<RosBagMetadataCache>(std::move(metadata), current_time);
}

RosBagReader::RosBagReader(ClientContext& context, RosReaderOptions options, string file_name):
    allocator(BufferAllocator::Get(context)), options(std::move(options))
{
	file_handle = FileSystem::GetFileSystem(context).OpenFile(file_name, FileFlags::FILE_FLAGS_READ);
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
}

RosBagReader::RosBagReader(ClientContext &context, RosReaderOptions options, shared_ptr<RosBagMetadataCache> metadata): 
    allocator(BufferAllocator::Get(context)), options(std::move(options)), metadata(std::move(metadata))
{
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

idx_t RosBagReader::NumChunks() const {
    return topic_index->chunks.size(); 
} 

idx_t RosBagReader::NumMessages() const {
    return topic_index->message_cnt; 
}

void RosBagReader::InitializeSchema() {
    message_def = MakeMsgDef(options.topic); 
    
    // This flag is set to true if we successfully split the header field
    // In this case we can ignore the heaer field when splitting the main message fields 
    bool header_split = false; 

    // If splt header option is enabled and there is a header, add header fields to schema
    // Lets dynamically add header fields to shema 
    if (options.split_header && (message_def->fieldIndexes()->count("header") > 0)) {
        auto header_idx = message_def->fieldIndexes()->at("header");
        auto header_mem = message_def->members().at(header_idx); 

        if ( header_mem.index() == 0) {
            auto header = std::get<RosMsgTypes::FieldDef>(header_mem); 
            if (header.type() ==  RosValue::Type::object) {
                header_split = true; 
                for (auto member : header.typeDefinition().members()) {
                    if (member.index() == 0) {
                        auto field = std::get<RosMsgTypes::FieldDef>(member); 
                        names.push_back("header." + field.name()); 
                        return_types.push_back(ConvertRosFieldType(field)); 
                    }
                }
            }
        } 
    }
    // Convert each ros message field into schema 
    for (auto member : message_def->members()) {
        if (member.index() == 0){ 
            auto field = std::get<RosMsgTypes::FieldDef>(member); 
            // Ignore header field if we've already split it. 
            if (!(header_split && field.name() == "header")) {
                 names.push_back(field.name()); 
                 return_types.push_back(ConvertRosFieldType(field)); 
            }
        }
    } 
}

shared_ptr<RosMsgTypes::MsgDef> RosBagReader::MakeMsgDef(const string &topic) const {
    const auto conn_data = GetMetadata().getConnectionData(topic); 
    return ParseMsgDef(conn_data.message_definition, conn_data.type); 
}

shared_ptr<RosBagReader::TopicIndex> RosBagReader::MakeTopicIndex(const std::string &topic) const {
    auto topic_idx = make_shared<RosBagReader::TopicIndex>(); 

    for (const auto &connection_record : metadata->metadata->topic_connection_map.at(topic)) {
        for (const auto &block : connection_record->blocks) {
          topic_idx->message_cnt += block.info.message_count; 
          topic_idx->chunks.emplace(block);
        }
        topic_idx->connection_ids.emplace(connection_record->id);
      }
    }   
}

