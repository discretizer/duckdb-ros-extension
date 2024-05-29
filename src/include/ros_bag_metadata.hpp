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
    RosBagMetadata() {
    }

    bool TopicInBag(const string &topic) const {
        return topic_connection_map.count(topic) != 0;
    }

    const unordered_set<string>& Topics() const {
        return topics; 
    }

    const RosBagTypes::connection_data_t& getConnectionData(const std::string &topic) const {
        const auto it = topic_connection_map.find(topic);
        if (it == topic_connection_map.end()) {
            throw std::runtime_error("Unable to find topic in bag: " + topic);
        }
        if (connections.empty()) {
            throw std::runtime_error("No connection data for topic: " + topic);
        }
        return connections[it->second[0]].data;
    }

    unordered_set<string> topics; 
    vector<RosBagTypes::connection_record_t> connections;
    unordered_map<string, vector<uint32_t>> topic_connection_map;
    
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