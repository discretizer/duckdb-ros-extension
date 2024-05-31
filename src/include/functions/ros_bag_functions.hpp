#pragma once 



#include "ros_bag_reader.hpp"
#include "duckdb/function/function_set.hpp"


namespace duckdb {

class RosBagInfoFunction : public TableFunction {
public:
	RosBagInfoFunction();
};

}