#include "ros_schema.hpp" 

namespace duckdb {

LogicalType primitiveToDuckType(const RosValue::Type& type) {
    switch(type) {
    case RosValue::Type::ros_bool: 
        return LogicalType::BOOLEAN; 
    case RosValue::Type::uint8: 
        return LogicalType::UTINYINT;
    case RosValue::Type::int8: 
        return LogicalType::TINYINT; 
    case RosValue::Type::uint16:
        return LogicalType::USMALLINT; 
    case RosValue::Type::int16: 
        return LogicalType::SMALLINT;
    case RosValue::Type::uint32:
        return LogicalType::UINTEGER; 
    case RosValue::Type::int32:
        return LogicalType::INTEGER; 
    case RosValue::Type::uint64: 
        return LogicalType::UBIGINT; 
    case RosValue::Type::int64: 
        return LogicalType::BIGINT; 
    case RosValue::Type::float32: 
        return LogicalType::FLOAT; 
    case RosValue::Type::float64:
        return LogicalType::DOUBLE; 
    case RosValue::Type::string:
        return LogicalType::VARCHAR;
    case RosValue::Type::ros_time:
        return LogicalType::TIMESTAMP_NS; 
    case RosValue::Type::ros_duration:
        return LogicalType::INTERVAL;
    default:
        throw std::runtime_error("Type is not a primative"); 
    } 
}

LogicalType ConvertRosFieldType(const RosMsgTypes::FieldDef& def) {
    LogicalType duck_type; 
    if (def.type() == RosValue::Type::object) {
        child_list_t<LogicalType> children; 
        auto members = def.typeDefinition().members(); 
        children.reserve(members.size()); 
        for (auto i = members.cbegin(); i != members.cend(); i++) {
            if (i->index() == 0) {
                auto field = std::get<RosMsgTypes::FieldDef>(*i); 
                children.emplace_back(std::make_pair(field.name(), ConvertRosFieldType(field))); 
            }
        }
        duck_type = LogicalType::STRUCT(children); 
    } else {
        duck_type = primitiveToDuckType(def.type()); 
    }
    if (def.arraySize() > 0) {
        duck_type = LogicalType::ARRAY(duck_type); 
    } else if (def.arraySize() == -1) {
        duck_type = LogicalType::LIST(duck_type); 
    } 
    return duck_type; 
}
}