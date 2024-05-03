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
	RosBagReaderScanState scan_state;
	bool is_parallel;
	idx_t batch_index;
	idx_t file_index;
};

/// @brief ROS Global reader state.  Ideally we'd split this bag reading into 
/// multiple threads (probably one per file) and leverage some parallelization to 
/// increase efficiency. For now, let's keep it simple. 
struct RosGlobalState : public GlobalTableFunctionState {
    mutex lock;

	//! The initial reader from the bind phase
	shared_ptr<RosBagReader> initial_reader;
	//! Currently opened readers
	vector<shared_ptr<RosBagReader>> readers;
	//! Flag to indicate a file is being opened
	vector<RosBagFileState> file_states;
	//! Mutexes to wait for a file that is currently being opened
	unique_ptr<mutex[]> file_mutexes;
	//! Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

	//! Index of file currently up for scanning
	atomic<idx_t> file_index;
	//! Index of row group within file currently up for scanning
	idx_t row_group_index;
	//! Batch index of the next row group to be scanned
	idx_t batch_index;

	/// @brief Maximum threads for the current reader
	/// @return Maximum threads.  Allways one for now 
	idx_t MaxThreads() const override {
		return max_threads;
	}
}; 


class RosScan

struct RosBindData : public TableFunctionData {
	shared_ptr<RosReader> initial_reader;

	vector<string> files;
	atomic<idx_t> chunk_count;
	vector<string> names;
	vector<LogicalType> types;

	// The union readers are created (when parquet union_by_name option is on) during binding
	// Those readers can be re-used during ParquetParallelStateNext
	vector<shared_ptr<RosReader>> union_readers;

	// These come from the initial_reader, but need to be stored in case the initial_reader is removed by a filter
	idx_t initial_file_cardinality;
	idx_t initial_file_row_groups;

	RosOptions parquet_options;
	MultiFileReaderBindData reader_bind;

	void Initialize(shared_ptr<RosReader> reader) {
		initial_reader = std::move(reader);
		initial_file_cardinality = initial_reader->NumRows();
		initial_file_row_groups = initial_reader->NumRowGroups();
		parquet_options = initial_reader->parquet_options;
	}
};

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