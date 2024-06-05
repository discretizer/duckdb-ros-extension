#include "duckdb.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

#include "ros_scanner.hpp"
#include "ros_bag_reader.hpp"

#include <optional> 

namespace duckdb {

struct RosGlobalState;
struct RosLocalState : public LocalTableFunctionState {
	shared_ptr<RosBagReader> reader;
	idx_t batch_index; 
	RosBagReader::ScanState scan_state; 
};

struct RosBindData : public TableFunctionData {
	unique_ptr<MultiFileList> file_list;
	unique_ptr<MultiFileReader> multi_file_reader;
	MultiFileReaderBindData reader_bind;
	
	shared_ptr<RosBagReader> initial_reader;
	RosReaderOptions ros_options;

	vector<string> names;
	vector<LogicalType> types;

	/// @brief cardinality of the initial bind file 
	idx_t initial_file_cardinality;

	/// @brief chunk count of the initial bind file 
	idx_t initial_file_chunk_count;

	/// @brief Read chunck count.  This is used for updating the progress. 
	atomic<idx_t> chunk_count;
	
};

enum class RosFileState : uint8_t { UNOPENED, OPENING, OPEN, CLOSED };

struct RosFileReaderData {
	// Create data for an unopened file
	explicit RosFileReaderData(const string &file_to_be_opened)
	    : reader(nullptr), file_state(RosFileState::UNOPENED), file_mutex(make_uniq<mutex>()),
	      file_to_be_opened(file_to_be_opened) {
	}
	// Create data for an existing reader
	explicit RosFileReaderData(shared_ptr<RosBagReader> reader_p)
	    : reader(std::move(reader_p)), file_state(RosFileState::OPEN), file_mutex(make_uniq<mutex>()) {
	}

	//! Currently opened reader for the file
	shared_ptr<RosBagReader> reader;
	//! Flag to indicate the file is being opened
	RosFileState file_state;
	//! Mutexes to wait for the file when it is being opened
	unique_ptr<mutex> file_mutex;

	//! (only set when file_state is UNOPENED) the file to be opened
	string file_to_be_opened;
};

/// @brief ROS Global reader state.  Ideally we'd split this bag reading into 
/// multiple threads and leverage some chunk indexing and parallelization to 
/// increase efficiency. 
/// We're going to attempt to use appropriate encapsulation for this class 
/// even though the underlying API really doesn't make this easy (or even possible)
class RosGlobalState : public GlobalTableFunctionState {
public: 

	// 
	RosGlobalState(ClientContext& context, const RosBindData &bind_data, const vector<column_t>& col_ids):
		initial_reader(bind_data.initial_reader), 
		readers(), 
		file_list_scan(), 
		multi_file_reader_state(bind_data.multi_file_reader->InitializeGlobalState(
		    context, bind_data.ros_options.file_options, bind_data.reader_bind, *bind_data.file_list,
		    bind_data.initial_reader->GetTypes(), bind_data.initial_reader->GetNames(), col_ids)),
		file_index(0), 
		chunk_index(), 
		column_ids(col_ids),
		max_threads(MaxThreadsHelper(context, bind_data)), 
		batch_index(0)
	{
		readers.reserve(bind_data.file_list->GetTotalFileCount()); 
		for (auto file: bind_data.file_list->Files()) {
			if (file == bind_data.file_list->GetFirstFile()) {
				readers.emplace_back(initial_reader); 
			} else {
				readers.emplace_back(file); 
			} 
		}
		bind_data.file_list->InitializeScan(file_list_scan); 
	}
	
	/// *** Overridden functions ** 
	/// @brief Maximum threads for the current reader
	/// @return Maximum threads. 
	idx_t MaxThreads() const override {
		return max_threads;
	}

	/// Populate the next local state from the 
	bool GetNext(ClientContext &context, const RosBindData &bind_data, RosLocalState& local_state) 
	{
		unique_lock global_lock(lock); 
		std::optional<bool> status; 
		do {
			if (error_opening_file) {
				status = false; 
			} else if (file_index >= readers.size() && ResizeFiles(bind_data)) {
				status = false; 
			} else if (readers[file_index].file_state == RosFileState::OPEN) {
				if (chunk_index != readers[file_index].reader->GetTopicChunkSet().cend()) {
					local_state.reader = readers[file_index].reader;
					// dequeue chunks until we're close too (but hopefullu still below the vector size)
					local_state.reader->InitializeScan(local_state.scan_state, chunk_index); 
					local_state.batch_index = batch_index++; 

					status =  true; 
				} else {
					readers[file_index].file_state = RosFileState::CLOSED; 
					readers[file_index].reader = nullptr; 
					chunk_index.reset(); 
					file_index++; 
				}
			} else if (TryOpenNextFile(context, bind_data, local_state, global_lock)) {
				; // Statement intentionally blank 
			} else 	if (readers[file_index].file_state == RosFileState::OPENING) {
				WaitForFile(global_lock); 
			}
		} while(!status.has_value()); 	
		return status.value(); 
	}

private: 
	static const idx_t MaxThreadsHelper( ClientContext& context, const RosBindData &bind_data) {
		if (bind_data.file_list->GetTotalFileCount() > 1) {
			return TaskScheduler::GetScheduler(context).NumberOfThreads();
		}
		return MaxValue(bind_data.initial_file_chunk_count, (idx_t)1);
	}

	// Queries the metadataprovider for another file to scan, updating the files/reader lists in the process.
	// Returns true if resized
	bool ResizeFiles(const RosBindData &bind_data) {
		string scanned_file;
		if (!bind_data.file_list->Scan(file_list_scan, scanned_file)) {
			return false;
		}

		// Push the file in the reader data, to be opened later
		readers.emplace_back(scanned_file);
		return true;
	}

	void WaitForFile(unique_lock<mutex>& global_lock) {
		bool done = false; 
		while (!done) {
			auto& file_mutex = *readers[file_index].file_mutex; 

			global_lock.unlock();
			unique_lock<mutex> current_file_lock(file_mutex); 
			global_lock.lock(); 

			done = (file_index >= readers.size()) || 
				   (readers[file_index].file_state != RosFileState::OPENING) || 
				   error_opening_file; 
		}
	}

	bool TryOpenNextFile(ClientContext& context, const RosBindData& bind_data, RosLocalState& local_state, unique_lock<mutex>& global_lock) {
		const auto num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
		const auto file_index_limit =
		    MinValue<idx_t>(file_index + num_threads, readers.size());
		
		for(idx_t i = file_index; i < file_index_limit; i++) {
			if (readers[i].file_state == RosFileState::UNOPENED) {
				auto &current_reader_data = readers[i];

				current_reader_data.file_state = RosFileState::OPENING; 
				auto ros_options = initial_reader->Options(); 

				auto &current_file_lock = *current_reader_data.file_mutex;

				global_lock.unlock(); 
				unique_lock<mutex> file_lock(current_file_lock); 
				shared_ptr<RosBagReader> reader; 
				try {
					reader = make_shared_ptr<RosBagReader>(context, ros_options, current_reader_data.file_to_be_opened); 
					bind_data.multi_file_reader->InitializeReader(*reader, ros_options.file_options, bind_data.reader_bind, 
													  initial_reader->GetTypes(), initial_reader->GetNames(), 
													  column_ids, filters, initial_reader->GetFileName(), context, multi_file_reader_state); 
				} catch (...) {
					global_lock.lock();
					error_opening_file = true;
					throw;
				}
				global_lock.lock();

				current_reader_data.reader = reader; 
				current_reader_data.file_state = RosFileState::OPEN;

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

	//! The initial reader from the bind phase
	shared_ptr<RosBagReader> initial_reader;

	//! Currently opened readers
	vector<RosFileReaderData> readers;

	//! File list scan options	
	MultiFileListScanData file_list_scan;

	//! Multi-file rader global state
	unique_ptr<MultiFileReaderGlobalState> multi_file_reader_state;
	
	//! Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

	//! Index of file currently up for scanning
	atomic<idx_t> file_index;

	//! Current chunk index location to read 
	std::optional<RosBagReader::ChunkSet::const_iterator> chunk_index;  

	//! Current column_ids (past in from input on creation)
	//! Used in MultiFileReader::InitializeReader
	vector<column_t> column_ids;

	//! Current table filter set (past in from input on creation)
	//! Used
	TableFilterSet *filters; 

	//! Maximum number of threads; 
	idx_t max_threads; 

	//! Batch index of the next chunk to be scanned
	idx_t batch_index;
}; 

static unique_ptr<FunctionData> RosBagBindInternal(ClientContext &context,
	                                               unique_ptr<MultiFileReader> multi_file_reader,
	                                               unique_ptr<MultiFileList> file_list,
	                                               vector<LogicalType> &return_types, vector<string> &names,
	                                               RosReaderOptions ros_options) 
{
	auto result = make_uniq<RosBindData>()
	;
	ros_options.file_options.AutoDetectHivePartitioning(*file_list, context); 
	if (ros_options.file_options.union_by_name) {
		throw BinderException("RosBag reading doesn't currently support union_by_name"); 
	}

	// Create initial reader.  This should initialize the schema.
	auto initial_reader = make_shared_ptr<RosBagReader>(context, ros_options, file_list->GetFirstFile());
	ros_options.split_header = initial_reader->Options().split_header; 

	names = initial_reader->GetNames(); 
	return_types = initial_reader->GetTypes();

	multi_file_reader->BindOptions(ros_options.file_options, *file_list, return_types, names, result->reader_bind ); 
	 
	result->multi_file_reader=std::move(multi_file_reader); 
	result->file_list = std::move(file_list);
	result->initial_file_chunk_count = initial_reader->GetTopicChunkSet().size(); 
	result->initial_file_cardinality = initial_reader->NumTopicMessages(); 
	result->initial_reader = initial_reader; 
	result->ros_options = ros_options;  
	
	return std::move(result); 

}

static unique_ptr<FunctionData> RosBagBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
	// Create output result
	auto result = make_uniq<RosBindData>();
	auto multi_file_reader = MultiFileReader::Create(input.table_function); 

	// Parse input options 
	auto file_list = multi_file_reader->CreateFileList(context, input.inputs[0]); 

	RosReaderOptions ros_options;
	for ( auto& kv: input.named_parameters) {
		if (multi_file_reader->ParseOption(kv.first, kv.second, ros_options.file_options, context)) {
			continue; 
		}
		auto loption = StringUtil::Lower(kv.first); 
		if (loption == "split_header") {
			ros_options.split_header = BooleanValue::Get(kv.second); 
		}
		if (loption == "rx_timestamp_col") {
			ros_options.rx_timestamp_col = StringValue::Get(kv.second); 
		}
		if (loption == "topic") {
			ros_options.topic = StringValue::Get(kv.second); 
		}
	}
	if(ros_options.topic == "") {
		throw BinderException("Must specify a topic to to the rosbag reader"); 
	}

	return RosBagBindInternal(context, std::move(multi_file_reader), std::move(file_list), return_types, names, ros_options); 
}

static unique_ptr<GlobalTableFunctionState> RosBagInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RosBindData>();

	if (bind_data.file_list->IsEmpty()) {
		// This can happen when a filename based filter pushdown has eliminated all possible files for this scan.
		return nullptr;
	}
	return make_uniq<RosGlobalState>(context, bind_data, input.column_ids);
}

static double RosBagProgress(ClientContext &context, const FunctionData *bind_data_p, 
	const GlobalTableFunctionState *global_state) {
	
	auto &bind_data = bind_data_p->Cast<RosBindData>();
	auto &gstate = global_state->Cast<RosGlobalState>();
	if (bind_data.file_list->IsEmpty()) {
		return 100.0;
	}
	if (bind_data.initial_file_cardinality == 0) {
		return (100.0 * (gstate.file_index + 1)) / bind_data.file_list->GetTotalFileCount();
	}
	auto percentage = MinValue<double>(
	    100.0, (bind_data.initial_file_chunk_count * STANDARD_VECTOR_SIZE * 100.0 / bind_data.initial_file_cardinality));
	return (percentage + 100.0 * gstate.file_index) / bind_data.file_list->GetTotalFileCount();
}

static unique_ptr<LocalTableFunctionState> RosBagInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     	   GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<RosBindData>();
	auto &gstate = global_state_p->Cast<RosGlobalState>();

	auto result = make_uniq<RosLocalState>();

	if (!gstate.GetNext(context.client, bind_data, *result)) {
		return nullptr; 
	}
	return std::move(result); 
}

static void RosBagScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    if (!data_p.local_state) {
        return;  
    }
    auto &data = data_p.local_state->Cast<RosLocalState>();
	auto &gstate = data_p.global_state->Cast<RosGlobalState>();

	auto &bind_data = data_p.bind_data->CastNoConst<RosBindData>();
	do {
		data.reader->Scan(data.scan_state, output); 
		if (output.size() > 0) {
			bind_data.multi_file_reader->FinalizeChunk(context, bind_data.reader_bind, data.reader->reader_data,
				                                   output, gstate.multi_file_reader_state);
			return;
		}
		if (!gstate.GetNext(context, bind_data, data)) {
			return;
		}
	} while(true); 
}
static unique_ptr<NodeStatistics> RosBagCardinality(ClientContext &context, const FunctionData *bind_data) {
	auto &data = bind_data->Cast<RosBindData>();
	return make_uniq<NodeStatistics>(data.initial_file_cardinality * data.file_list->GetTotalFileCount());
}

static idx_t RosBagScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	                                  LocalTableFunctionState *local_state,
	                                  GlobalTableFunctionState *global_state) {
	auto &data = local_state->Cast<RosLocalState>();
	return data.batch_index;
}

static void RosScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                                 const TableFunction &function) {
	auto &bind_data = bind_data_p->Cast<RosBindData>();

	serializer.WriteProperty(100, "files", bind_data.file_list->GetAllFiles());
	serializer.WriteProperty(101, "types", bind_data.types);
	serializer.WriteProperty(102, "names", bind_data.names);
	serializer.WriteProperty(103, "ros_options", bind_data.ros_options);
}

static unique_ptr<FunctionData> RosScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	auto &context = deserializer.Get<ClientContext &>();
	auto files = deserializer.ReadProperty<vector<string>>(100, "files");
	auto types = deserializer.ReadProperty<vector<LogicalType>>(101, "types");
	auto names = deserializer.ReadProperty<vector<string>>(102, "names");
	auto ros_options = deserializer.ReadProperty<RosReaderOptions>(103, "ros_options");

	vector<Value> file_path;
	for (auto &path : files) {
		file_path.emplace_back(path);
	}

	auto multi_file_reader = MultiFileReader::Create(function);
	auto file_list = multi_file_reader->CreateFileList(context, Value::LIST(LogicalType::VARCHAR, file_path),
		                                                   FileGlobOptions::DISALLOW_EMPTY);
	return RosBagBindInternal(context, std::move(multi_file_reader), std::move(file_list), types, names,
		                               ros_options);
}

TableFunctionSet RosScanFunction::GetFunctionSet() {
    TableFunction table_function("ros_scan", {LogicalType::VARCHAR}, RosBagScanImplementation, RosBagBind, RosBagInitGlobal, RosBagInitLocal); 
	table_function.table_scan_progress = RosBagProgress; 
	table_function.cardinality = RosBagCardinality;
	table_function.get_batch_index = RosBagScanGetBatchIndex;
	table_function.serialize = RosScanSerialize;
	table_function.deserialize = RosScanDeserialize;

	RosReaderOptions::AddParameters(table_function); 
	MultiFileReader::AddParameters(table_function);

	return MultiFileReader::CreateFunctionSet(table_function); 
}


}