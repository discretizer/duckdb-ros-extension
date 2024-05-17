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



/// @brief ROS Global reader state. 
struct RosGlobalState : public GlobalTableFunctionState {
    mutex lock;

	/// @brief The initial reader from the bind phase
	shared_ptr<RosBagReader> initial_reader;

	/// @brief Currently opened readers
	vector<shared_ptr<RosBagReader>> readers;
	
	/// @brief Flag to indicate a file is being opened
	//vector<RosBagFileState> file_states;

	/// @brief Mutexes to wait for a file that is currently being opened
	unique_ptr<mutex[]> file_mutexes;
	
	/// @brief Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

	/// @brief Index of file currently up for scanning
	atomic<idx_t> file_index;

	/// @brief Maximum threads for the current reader
	/// @return Maximum threads. 
	idx_t MaxThreads() const override {
		return max_threads;
	}
}; 


struct RosBindData : public TableFunctionData {
	void Initialize(shared_ptr<RosBagReader> reader) {
		initial_reader = std::move(reader);
		ros_options = initial_reader->Options();
	}

	shared_ptr<RosBagReader> initial_reader;
	vector<string> files;

	RosOptions ros_options;
	atomic<idx_t> chunk_count;

	MultiFileReaderBindData reader_bind;
};


static unique_ptr<FunctionData> RosBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
	input.inputs[0]
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
	    100.0, (bind_data.chunk_count * STANDARD_VECTOR_SIZE * 100.0 / bind_data.initial_file_cardinality));
	return (percentage + 100.0 * gstate.file_index) / bind_data.files.size();
}

/// @brief 
/// @param context 
/// @param data_p 
/// @param output 
static void RosScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	// Get current state information for this thread
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