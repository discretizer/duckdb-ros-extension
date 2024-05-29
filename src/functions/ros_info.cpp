#include "functions/ros_bag_info.hpp"

#include <sstream>

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/config.hpp"
#endif

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
}; 

//===--------------------------------------------------------------------===//
// Row Group Meta Data
//===--------------------------------------------------------------------===//
void RosBagInfoOperatorData::BindMetaData(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("file_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("duration");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("start");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("end");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("messages");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("compression");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("types");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("topics");
	return_types.emplace_back(LogicalType::BIGINT);
}


}