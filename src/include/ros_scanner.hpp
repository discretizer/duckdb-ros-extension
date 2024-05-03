#pragma once 

#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

struct RosOptions {
    explicit RosOptions() {
	}
	explicit RosOptions(ClientContext &context);

    string topic; 
    bool decode_message;  
}; 

class RosScanFunction {
public: 
    static TableFilterSet

};

}

