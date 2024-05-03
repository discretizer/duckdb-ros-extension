#pragma once 

#include "duckdb.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#endif

#include "ros_bag_types.hpp"
#include "ros_msg_types.hpp"

namespace duckdb {

class RosBagMetadata {
public: 
    RosBagMetadata(); 

    bool TopicInBag(const string &topic) const {
        return topic_connection_map.count(topic) != 0;
    }

    const unordered_set<string>& Topics() const {
        return topics; 
    }

    unordered_set<string> topics; 
    vector<RosBagTypes::connection_record_t> connections;
    
    unordered_map<string, vector<RosBagTypes::connection_record_t&>> topic_connection_map;
    unordered_map<string, shared_ptr<RosMsgTypes::MsgDef>> message_schemata;

    vector<RosBagTypes::chunk_info_t> chunk_infos;
    vector<RosBagTypes::chunk_t> chunks;
}; 

/// @brief Metadata cache entry object for RosBag metadata
class RosBagMetadataCache : public ObjectCacheEntry {
public:
	RosBagMetadataCache() : metadata(nullptr) {
	}
	RosBagMetadataCache(unique_ptr<RosBagMetadata> file_metadata, time_t r_time)
	    : metadata(std::move(file_metadata)), read_time(r_time) {
	}

	~RosBagMetadataCache() override = default;

	//! Ros Bag metadata
	unique_ptr<const RosBagMetadata> metadata;

	//! read time
	time_t read_time;
	
public:
	static string ObjectType() {
		return "ros_bag_metadata";
	}

	string GetObjectType() override {
		return ObjectType();
	}
};

}