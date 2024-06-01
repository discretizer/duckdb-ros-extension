#include "functions/ros_bag_functions.hpp"

#include "duckdb.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/config.hpp"
#endif

#include "ros_bag_metadata.hpp"
#include <sstream>

namespace duckdb {

struct TypeInfo {
    string md5; 
    string definition; 
}; 

struct TopicInfo {
    uint32_t messages; 
    string type_name; 
}; 

struct RosBagInfoBindData : public TableFunctionData {
	vector<LogicalType> return_types;
	unique_ptr<MultiFileList> file_list;
	unique_ptr<MultiFileReader> multi_file_reader;
};

enum class RosBagInfoOperatorType : uint8_t { INFO, CHUNKS, CONNECTIONS };
struct RosBagInfoOperatorData : public GlobalTableFunctionState {
    explicit RosBagInfoOperatorData(ClientContext &context, const vector<LogicalType> &types)
        : collection(context, types)
    {}
	

	ColumnDataCollection collection;
	ColumnDataScanState scan_state;

    MultiFileListScanData file_list_scan;
	string current_file;

public:
    static void BindInfoData(vector<LogicalType> &return_types, vector<string> &names);
    //static void BindChunkData(vector<LogicalType> &return_typs, vector<string> &names);
    //static void BindConnectionData(vector<LogicalType> &return_typs, vector<string> &names);
     
    void LoadBagInfoData(ClientContext& context, const vector<LogicalType>& return_types, const string &file_path); 
    //void LoadBagChunkData(ClientContext& context, const vector<LogicalType>& return_types, const string &file_path);
    //void LoadBagConnectionData(ClientContext& context, const vector<LogicalType>& return_types, const string &file_path);
}; 

//===--------------------------------------------------------------------===//
// Row Group Meta Data
//===--------------------------------------------------------------------===//
void RosBagInfoOperatorData::BindInfoData(vector<LogicalType> &return_types, vector<string> &names) {
    names.emplace_back("file_name");
	return_types.emplace_back(LogicalTypeId::VARCHAR);

	names.emplace_back("duration");
	return_types.emplace_back(LogicalTypeId::INTERVAL);

	names.emplace_back("start");
	return_types.emplace_back(LogicalTypeId::TIMESTAMP_NS);

	names.emplace_back("end");
	return_types.emplace_back(LogicalTypeId::TIMESTAMP_NS);

	names.emplace_back("messages");
	return_types.emplace_back(LogicalTypeId::UBIGINT);

    names.emplace_back("chunks");
    return_types.emplace_back(LogicalTypeId::UBIGINT);

    vector<pair<string, LogicalType>> compression_stats_children = {
        {"type", LogicalTypeId::VARCHAR}, 
        {"count", LogicalTypeId::UBIGINT}
    }; 

	names.emplace_back("compression");
	return_types.emplace_back(LogicalType::LIST(LogicalType::STRUCT(compression_stats_children)));

    vector<pair<string, LogicalType>> rostype_children = {
        {"name", LogicalTypeId::VARCHAR}, 
        {"md5_sum", LogicalTypeId::VARCHAR}, 
        {"definiton", LogicalTypeId::VARCHAR}};  
    LogicalType rostype = LogicalType::STRUCT(rostype_children);  

    names.emplace_back("types");
	return_types.emplace_back(LogicalType::LIST(rostype));

	vector<pair<string, LogicalType>> topic_children = {
        {"name", LogicalTypeId::VARCHAR}, 
        {"type", LogicalTypeId::VARCHAR},
        {"messages", LogicalTypeId::UBIGINT}};  

    LogicalType topictype = LogicalType::STRUCT(topic_children); 

    names.emplace_back("topics");    
	return_types.emplace_back(LogicalType::LIST(topictype));
}

void RosBagInfoOperatorData::LoadBagInfoData(ClientContext& context, const vector<LogicalType>& return_types, const string &file_path) {
    RosReaderOptions options; 
    auto reader = make_uniq<RosBagReader>(context, options, file_path); 

    auto& metadata = reader->GetMetadata();

    constexpr uint32_t MAX_UINT32 = std::numeric_limits<uint32_t>::max();

    RosValue::ros_time_t start_time = RosValue::ros_time_t(MAX_UINT32,MAX_UINT32); 
    RosValue::ros_time_t end_time = RosValue::ros_time_t(0, 0); 

    map<string, TopicInfo> topic_stats;
    map<string, TypeInfo> type_stats;  

    for (const auto& connection: metadata.connections) {
        auto& current_topic_stat = topic_stats[connection.topic]; 
        
        // TODO: throw some kind of warning if topic connection types don't match
        current_topic_stat.type_name = connection.data.type; 
        current_topic_stat.messages += connection.data.message_count; 

        auto& current_type_stat = type_stats[connection.data.type]; 
        
        if (connection.data.message_definition.size() > current_type_stat.definition.size()) {
            current_type_stat.definition = connection.data.message_definition; 
            current_type_stat.md5 = connection.data.md5sum; 
        }
    }
    map<string, uint32_t> chunk_compression_stats; 
    uint32_t total_messages = 0; 
    for (const auto& chunk: metadata.chunks ) {
        start_time = MinValue(chunk.info.start_time, start_time); 
        end_time = MaxValue(chunk.info.end_time, end_time); 
        chunk_compression_stats[chunk.compression]++; 
        total_messages += chunk.info.message_count;
    }

	DataChunk current_chunk;
	current_chunk.Initialize(context, return_types, 1);
    current_chunk.SetValue(0, 0, Value(reader->GetFileName())); //Return file name 
    current_chunk.SetValue(0, 1, Value::INTERVAL(0, 0, (end_time.to_nsec() - start_time.to_nsec()) * 1e3)); 
    current_chunk.SetValue(0, 2, Value::TIMESTAMPNS(timestamp_t(start_time.to_nsec())));
    current_chunk.SetValue(0, 3, Value::TIMESTAMPNS(timestamp_t(end_time.to_nsec()))); 
    current_chunk.SetValue(0, 4, Value::UBIGINT(total_messages));
    current_chunk.SetValue(0, 5, Value::UBIGINT(metadata.chunks.size()));

    vector<Value> compression_list; 
    for (const auto& stat: chunk_compression_stats) {
        compression_list.emplace_back(Value::STRUCT({
            {"type", Value(stat.first)}, 
            {"count", Value::UBIGINT(stat.second)}
        })); 
    }
    current_chunk.SetValue(0, 6, Value::LIST(compression_list));

    vector<Value> type_list; 
    for (const auto& stat: type_stats ) {
        type_list.emplace_back(Value::STRUCT({
            {"name", Value(stat.first)},
            {"md5_sum", Value(stat.second.md5)},
            {"definition", Value(stat.second.definition)}
        })); 
    }
    current_chunk.SetValue(0, 7, Value::LIST(type_list));

    vector<Value> topic_list; 
    for (const auto& stats: topic_stats ) {
        topic_list.emplace_back(Value::STRUCT({
            {"name", Value(stats.first)}, 
            {"type", Value(stats.second.type_name)}, 
            {"messages", Value::UBIGINT(stats.second.messages)}
        })); 
    }
}

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//
template <RosBagInfoOperatorType TYPE>
static unique_ptr<FunctionData> RosBagBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	switch (TYPE) {
	case RosBagInfoOperatorType::INFO:
        RosBagInfoOperatorData::BindInfoData(return_types, names); 
		break;
	default:
		throw InternalException("Unsupported ParquetMetadataOperatorType");
	}

	auto result = make_uniq<RosBagInfoBindData>();
	result->return_types = return_types;
	result->multi_file_reader = MultiFileReader::Create(input.table_function);
	result->file_list = result->multi_file_reader->CreateFileList(context, input.inputs[0]);
	return std::move(result);
}


template <RosBagInfoOperatorType TYPE>
static unique_ptr<GlobalTableFunctionState> RosBagInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RosBagInfoBindData>();
	auto result = make_uniq<RosBagInfoOperatorData>(context, bind_data.return_types);

	bind_data.file_list->InitializeScan(result->file_list_scan);
	bind_data.file_list->Scan(result->file_list_scan, result->current_file);

	D_ASSERT(!bind_data.file_list->IsEmpty());

	switch (TYPE) {
	case RosBagInfoOperatorType::INFO:
		result->LoadBagInfoData(context, bind_data.return_types, bind_data.file_list->GetFirstFile());
		break;
	default:
		throw InternalException("Unsupported RosBagInfoOperatorType");
	}

	return std::move(result);
}

template <RosBagInfoOperatorType TYPE>
static void RosBagImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<RosBagInfoOperatorData>();
	auto &bind_data = data_p.bind_data->Cast<RosBagInfoBindData>();

	while (true) {
        if (!data.collection.Scan(data.scan_state, output)) {
    		if (!bind_data.file_list->Scan(data.file_list_scan, data.current_file)) {
	    		return;
		    }

    		switch (TYPE) {
	    	case RosBagInfoOperatorType::INFO:
		    	data.LoadBagInfoData(context, bind_data.return_types, data.current_file);
			    break;
		    default:
			    throw InternalException("Unsupported RosBagInfoOperatorType");
		    }
		    continue;
            
        }
        if (output.size() != 0) {
			return;
        }
	}
}


RosBagInfoFunction::RosBagInfoFunction()
    : TableFunction("rosbag_info", {LogicalType::VARCHAR},
                    RosBagImplementation<RosBagInfoOperatorType::INFO>,
                    RosBagBind<RosBagInfoOperatorType::INFO>,
                    RosBagInit<RosBagInfoOperatorType::INFO>) {
}
}