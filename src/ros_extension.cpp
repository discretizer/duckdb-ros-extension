#define DUCKDB_EXTENSION_MAIN

#include "ros_extension.hpp"
#include "ros_scanner.hpp"

#include "functions/ros_bag_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

/**
inline void RosScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Ros "+name.GetString()+" 🐥");;
        });
}
**/

static void LoadInternal(DatabaseInstance &instance) {

    // Register a scalar function
    //auto ros_scalar_function = ScalarFunction("ros", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RosScalarFun);
    //ExtensionUtil::RegisterFunction(instance, ros_scalar_function);

    auto scan_fun = RosScanFunction::GetFunctionSet();
	scan_fun.name = "read_rosbag";
	ExtensionUtil::RegisterFunction(instance, scan_fun);
	scan_fun.name = "rosbag_scan";
	ExtensionUtil::RegisterFunction(instance, scan_fun);
    
    RosBagInfoFunction rosbag_info_function; 
    ExtensionUtil::RegisterFunction(instance, rosbag_info_function); 
}

void RosExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string RosExtension::Name() {
	return "ros";
}
} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void ros_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::RosExtension>();
}

DUCKDB_EXTENSION_API const char *ros_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
