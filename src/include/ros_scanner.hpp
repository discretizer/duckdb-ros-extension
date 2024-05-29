#pragma once 

#include "duckdb.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/function/scalar_function.hpp"
#endif

namespace duckdb {

class RosScanFunction {
public: 
    static TableFunctionSet GetFunctionSet(); 
};

}

