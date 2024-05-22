#include "duckdb.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

#include "ros_bag_reader.hpp"

namespace duckdb {

struct RosGlobalState;
struct RosLocalState : public LocalTableFunctionState {
	shared_ptr<RosBagReader> reader;
	vector<std::reference_wrapper<RosBagReader::ChunkIndex::reference>> chunk_list; 
};

struct RosBindData : public TableFunctionData {
	shared_ptr<RosBagReader> initial_reader;
	RosReaderOptions ros_options;

	/// @brief cardinality of the initial bind file 
	idx_t initial_file_cardinality;

	/// @brief chunk count of the initial bind file 
	idx_t initial_file_chunk_count;

	/// @brief Read chunck count.  This is used for updating the progress. 
	atomic<idx_t> chunk_count;

	vector<string> files;
	MultiFileReaderBindData reader_bind;


	// Initialization function.  This function is used by 
	void Initialize(shared_ptr<RosBagReader> reader) 
	 {
		chunk_count = 0; 
		initial_reader = std::move(reader);
		ros_options = initial_reader->Options();

		initial_file_cardinality = initial_reader->NumMessages(); 
		initial_file_chunk_count = initial_reader->NumChunks(); 
	}
};

enum class RosFileState : uint8_t { UNOPENED, OPENING, OPEN, CLOSED };


/// @brief ROS Global reader state.  Ideally we'd split this bag reading into 
/// multiple threads and leverage some chunk indexing and parallelization to 
/// increase efficiency. 

/// We're going to attempt to use appropriate encapsulation for this class 
/// even though the underlying API really doesn't make this easy (or even possible)
class RosGlobalState : public GlobalTableFunctionState {
public: 

	// 
	RosGlobalState(ClientContext& context, const RosBindData &bind_data, const vector<column_t>& col_ids):
		initial_reader(bind_data.initial_reader), readers(bind_data.files.size(), nullptr), 
		file_states(bind_data.files.size(), RosFileState::UNOPENED),
		file_mutexes(make_uniq_array<mutex>(bind_data.files.size())), 
		column_ids(col_ids),
		max_threads(MaxThreadsHelper(context, bind_data))
	{
	}
	
	/// *** Overridden functions ** 
	/// @brief Maximum threads for the current reader
	/// @return Maximum threads. 
	idx_t MaxThreads() const override {
		return max_threads; 
	}

	/// Populate the next local state from the 
	bool GetNext(ClientContext &context, const RosBindData &bind_data, RosLocalState& scan_state) 
	{
		unique_lock global_lock(lock); 

		std::optional<bool> result; 
		do {
			if (error_opening_file) {
				result = false; 
			} else if (file_index >= readers.size()) {
				result = false; 
			} else if (file_states[file_index] == RosFileState::OPEN) {
				if (index_pos != readers[file_index]->GetChunkIndex().cend()) {
					scan_state.reader = readers[file_index];

					decltype(scan_state.chunk_list) chunk_list{*index_pos};
					scan_state.chunk_list = std::move(chunk_list);
					index_pos++; 

					result = true; 
				} else {
					file_states[file_index] = RosFileState::CLOSED; 
					readers[file_index] = nullptr; 
					file_index++; 
					if (file_index >= bind_data.files.size()) {
						result = false; 
					}
				}
			} else if (TryOpenNextFile(context, bind_data, scan_state, global_lock)) {
				; // Statement intentionally blank 
			} else {
				if (file_states[file_index] == RosFileState::OPENING) {
					WaitForFile(global_lock); 
				}
			}
		} while(!result.has_value()); 	
		return result.value(); 
	}

private: 
	static const idx_t MaxThreadsHelper( ClientContext& context, const RosBindData &bind_data) {
		if (bind_data.files.size() > 1) {
			return TaskScheduler::GetScheduler(context).NumberOfThreads();
		}
		return MaxValue(bind_data.initial_file_chunk_count, (idx_t)1);
	}

	void WaitForFile(unique_lock<mutex>& global_lock) {
		bool done = false; 
		while (!done) {
			global_lock.unlock();
			unique_lock<mutex> current_file_lock(file_mutexes[file_index]); 
			global_lock.lock(); 

			done = (file_index >= readers.size()) || 
				   (file_states[file_index] != RosFileState::OPENING) || 
				   error_opening_file; 
		}
	}

	bool TryOpenNextFile(ClientContext& context, const RosBindData& bind_data, RosLocalState& scan_data, unique_lock<mutex>& global_lock) {
		const auto num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads(); 
		const auto file_index_limit = MinValue<idx_t>(file_index + num_threads, bind_data.files.size()); 

		for (idx_t i = file_index; i < file_index_limit; i++ ) {
			if (file_states[i] == RosFileState::UNOPENED) {
				string file = bind_data.files[i]; 
				file_states[i] = RosFileState::OPENING; 
				auto ros_options = initial_reader->Options(); 

				global_lock.unlock(); 
				unique_lock<mutex> file_lock(file_mutexes[i]); 
				std::shared_ptr<RosBagReader> reader; 
				try {
					reader = make_shared<RosBagReader>(context, file, ros_options); 
					MultiFileReader::InitializeReader(*reader, ros_options.file_options, bind_data.reader_bind, 
													  initial_reader->GetTypes(), initial_reader->GetNames(), 
													  column_ids, filters, initial_reader->GetFileName(), context); 
				} catch (...) {
					global_lock.lock();
					error_opening_file = true;
					throw;
				}
				global_lock.lock();
				readers[i] = reader;
				file_states[i] = RosFileState::OPEN;
				index_pos = reader->GetChunkIndex().cbegin(); 

				return true;
			}
		}
		return false; 
	}

// Data elements public for now - some of these NEED to be public for some of the multi-file 
// reader functions to actually work.  In the end, I believe these should be refactored on
// the backend, but I'm not going to do this. 
public: 
    //! Global state mutex lock
	mutex lock;

	//! The initial reader from the bind pha
	shared_ptr<RosBagReader> initial_reader;

	//! Currently opened readers
	vector<shared_ptr<RosBagReader>> readers;
	
	//! Flag to indicate a file is being opened
	vector<RosFileState> file_states; 
	
	//! Mutexes to wait for a file that is currently being opened
	unique_ptr<mutex[]> file_mutexes;
	
	//! Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

	//! Index of file currently up for scanning
	atomic<idx_t> file_index;

	//! Current index location to read 
	RosBagReader::ChunkIndex::const_iterator index_pos;  

	//! Current column_ids (past in from input on creation)
	//! Used in MultiFileReader::InitializeReader
	vector<column_t> column_ids;

	//! Current table filter set (past in from input on creation)
	//! Used
	TableFilterSet *filters; 

	//! Maximum number of threads; 
	idx_t max_threads; 
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
	if (ros_options.file_options.union_by_name) {
		throw BinderException("RosBag reading doesn't currently support union_by_name"); 
	}

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
	return make_uniq<RosGlobalState>(context, bind_data, input.column_ids);
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

static unique_ptr<LocalTableFunctionState> RosBagInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     	   GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<RosBindData>();
	auto &gstate = global_state_p->Cast<RosGlobalState>();

	auto result = make_uniq<RosLocalState>();

	if (gstate.GetNext(context.client, bind_data, *result)) {
		return nullptr; 
	}
	return std::move(result); 
}

static void RosScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    if (!data_p.local_state) {
        return;  
    }
    auto &data = data_p.local_state->Cast<RosLocalState>();
	auto &gstate = data_p.global_state->Cast<RosGlobalState>();

	auto &bind_data = data_p.bind_data->CastNoConst<RosBindData>();
	do {
		data.reader->Scan()
	}
}

static TableFunctionSet GetFunctionSet() {
    TableFunction table_function("ros_scan", {LogicalType::VARCHAR}, RosScanImplementation, RosScanBind)
}


}