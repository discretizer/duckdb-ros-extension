#include "functions/ros_bag_info.hpp"

#include <sstream>

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/config.hpp"
#endif

#include "ros_bag_metadata.hpp"

namespace duckdb {

struct RosBagInfoBindData : public TableFunctionData {
	vector<LogicalType> return_types;
	unique_ptr<MultiFileList> file_list;
	unique_ptr<MultiFileReader> multi_file_reader;
};


struct RosBagInfoOperatorData : public GlobalTableFunctionState {
    explicit RosBagInfoOperatorData(ClientContext &context, const vector<LogicalType> &types) 
    {}
	
    MultiFileListScanData file_list_scan;
	string current_file;

public:
    static void BindInfoData(vector<LogicalType> &return_types, vector<string> &names); 

    void LoadBagInfoData(ClientContext& context, const vector<LogicalType>& return_types, const string &file_path); 
}; 

//===--------------------------------------------------------------------===//
// Row Group Meta Data
//===--------------------------------------------------------------------===//
void RosBagInfoOperatorData::BindInfoData(vector<LogicalType> &return_types, vector<string> &names) {
    names.emplace_back("file_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("duration");
	return_types.emplace_back(LogicalType::INTERVAL);

	names.emplace_back("start");
	return_types.emplace_back(LogicalType::TIMESTAMP_NS);

	names.emplace_back("end");
	return_types.emplace_back(LogicalType::TIMESTAMP_NS);

	names.emplace_back("messages");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("compression");
	return_types.emplace_back(LogicalType::VARCHAR);

    vector<pair<string, LogicalType>> rostype_children; 
    rostype_children.emplace_back(make_pair("name", LogicalType::VARCHAR));
    rostype_children.emplace_back(make_pair("md5_sum", LogicalType::VARCHAR));
    rostype_children.emplace_back(make_pair("definiton", LogicalType::VARCHAR));  
    
    LogicalType rostype = LogicalType::STRUCT(rostype_children);  

    names.emplace_back("types");
	return_types.emplace_back(LogicalType::LIST(rostype));

	vector<pair<string, LogicalType>> topic_children; 
    topic_children.emplace_back(make_pair("name", LogicalType::VARCHAR));
    topic_children.emplace_back(make_pair("type", LogicalType::VARCHAR));
    topic_children.emplace_back(make_pair("messages", LogicalType::BIGINT));  

    LogicalType topictype = LogicalType::STRUCT(topic_children); 

    names.emplace_back("topics");    
	return_types.emplace_back(LogicalType::LIST(topictype));
}

void RosBagInfoOperatorData::LoadBagInfoData(ClientContext& context, const vector<LogicalType>& return_types, const string &file_path) {
    RosReaderOptions options; 
    auto reader = make_uniq<RosBagReader>(context, file_path, options); 

    auto& metadata = reader->GetMetadata();
    vector<string> topic_names;

    topic_names.reserve(metadata.Topics().size());  
    for (auto topic: metadata.Topics()) {
        topic_names.emplace_back(topic); 
    }

    for (auto connection: metadata.connections) {
        
    }
}


}