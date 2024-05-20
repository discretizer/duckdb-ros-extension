#include "duckdb.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

#include "ros_bag_reader.hpp"

namespace duckdb {

struct RosGlobalState;

struct RosLocalState : public LocalTableFunctionState {
	shared_ptr<RosBagReader> reader;
};

/// @brief ROS Global reader state.  Ideally we'd split this bag reading into 
/// multiple threads and leverage some chunk indexing and parallelization to 
/// increase efficiency. 
struct RosGlobalState : public GlobalTableFunctionState {
    //! Global state mutex lock
	mutex lock;

	//! The initial reader from the bind pha
	shared_ptr<RosBagReader> initial_reader;

	//! Currently opened readers
	vector<shared_ptr<RosBagReader>> readers;
	
	//! Flag to indicate a file is being opened
	//vector<RosBagFileState> file_states;

	//! Mutexes to wait for a file that is currently being opened
	unique_ptr<mutex[]> file_mutexes;
	
	//! Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

	//! Index of file currently up for scanning
	atomic<idx_t> file_index;

	/// @brief Maximum threads for the current reader
	/// @return Maximum threads. 
	idx_t MaxThreads() const override {
		auto &bind_data = state.bind_data;

		if (!readers.empty() && state.readers[0]->HasFileHandle()) {
			// We opened and auto-detected a file, so we can get a better estimate
			auto &reader = *state.readers[0];
			return MaxValue<idx_t>(state.json_readers[0]->GetFileHandle().FileSize() / bind_data.maximum_object_size,
			                       1);
	
		}
		return state.system_threads;
	
		// One reader per file
		return bind_data.files.size();
	}
}; 

struct RosBindData : public TableFunctionData {
	shared_ptr<RosBagReader> initial_reader;
	RosReaderOptions ros_options;

	idx_t initial_file_cardinality;
	idx_t initial_file_chunk_count;
	atomic<idx_t> chunk_count;

	vector<string> files;
	MultiFileReaderBindData reader_bind;

	
	void Initialize(shared_ptr<RosBagReader> reader) {
		initial_reader = std::move(reader);
		ros_options = initial_reader->Options();

		initial_file_cardinality = initial_reader->NumMessages(); 
		initial_file_chunk_count = initial_reader->NumChunks(); 
	}
};

static unique_ptr<FunctionData> RosBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
	// Create output result
	auto result = make_uniq<RosBindData>();
	
	// Parse input options 
	auto files = MultiFileReader::GetFileList(context, input.inputs[0], "RosBag"); 
	RosReaderOptions ros_options;
	ros_options.topic = StringValue::Get(input.inputs[1]); 
	for ( auto& kv: input.named_parameters) {
		if (MultiFileReader::ParseOption(kv.first, kv.second, ros_options.file_options, context)) {
			continue; 
		}
		auto loption = StringUtil::Lower(kv.first); 
		if (loption == "split_header") {
			ros_options.split_header = BooleanValue::Get(kv.second); 
		}
	}
	ros_options.file_options.AutoDetectHivePartitioning(files, context); 
	
	// Create initial reader.  This should initialize the schema.
	auto initial_reader = make_shared<RosBagReader>(context, ros_options, files[0]);

	std::copy(initial_reader->GetNames().cbegin(), initial_reader->GetNames().cend(), names.end());
	std::copy(initial_reader->GetTypes().cbegin(), initial_reader->GetTypes().cend(), return_types.end()); 

	result->reader_bind = MultiFileReader::BindOptions(ros_options.file_options, files, return_types, names ); 
	return std::move(result); 
}


static unique_ptr<GlobalTableFunctionState> ReadRosBagInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RosBindData>();

	if (bind_data.files.empty()) {
		// This can happen when a filename based filter pushdown has eliminated all possible files for this scan.
		return nullptr;
	}
	return make_uniq<RosGlobalState>(context, bind_data.ros_options, context.db->NumberOfThreads(), bind_data.files, bind_data);
}

static double RosProgress(ClientContext &context, const FunctionData *bind_data_p, 
	const GlobalTableFunctionState *global_state) {
	
	auto &bind_data = bind_data_p->Cast<RosBindData>();
	auto &gstate = global_state->Cast<RosGlobalState>();
	if (bind_data.files.empty()) {
		return 100.0;
	}
	if (bind_data.initial_file_cardinality == 0) {
		return (100.0 * (gstate.file_index + 1)) / bind_data.files.size();
	}
	auto percentage = MinValue<double>(
	    100.0, (bind_data.initial_file_chunk_count * STANDARD_VECTOR_SIZE * 100.0 / bind_data.initial_file_cardinality));
	return (percentage + 100.0 * gstate.file_index) / bind_data.files.size();
}

static void RosScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    if (!data_p.local_state) {
        return;  
    }
    auto &data = data_p.local_state->Cast<RosLocalState>();
	auto &gstate = data_p.global_state->Cast<RosGlobalState>();

	auto &bind_data = data_p.bind_data->CastNoConst<RosBindData>();
	do {
    }
}

static TableFunctionSet GetFunctionSet() {
    TableFunction table_function("ros_scan", {LogicalType::VARCHAR}, RosScanImplementation, RosScanBind)
}


}