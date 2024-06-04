#include "ros_transform.hpp" 

namespace duckdb {

const string HEADER_NAME = "header"; 
const string HEADER_COL_PREFIX = "header.";

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

LogicalType RosTransform::ConvertRosFieldType(const RosMsgTypes::FieldDef& def) {
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

template <class T>
static bool TransformNumerical( const vector<RosValue::Pointer>& value_list, Vector &result);
static bool TransformTime(const vector<RosValue::Pointer>& value_list, Vector& result);
static bool TransformTime(const vector<RosValue::ros_time_t>& value_list, Vector& result);
static bool TransformInterval(const vector<RosValue::Pointer>& value_list, Vector& result);
static bool TransformString(const vector<RosValue::Pointer>& value_list, Vector& result);
static bool TransformArrayToArray(const vector<RosValue::Pointer>& value_list, Vector& result);
static bool TransformArrayToList(const vector<RosValue::Pointer>& value_list, Vector& result);
static bool TransformObjectToStruct(const vector<RosValue::Pointer>& value_list, Vector& result);

static bool TransformValues(const vector<RosValue::Pointer>& value_list, Vector& result) {
    // Sniff first element.  All should have the same type anyway. 
    switch (value_list[0]->getType()) {
    case RosValue::Type::uint8:
        return TransformNumerical<uint8_t>(value_list, result); 
    case RosValue::Type::uint16 :
        return TransformNumerical<uint16_t>(value_list, result); 
    case RosValue::Type::uint32:
        return TransformNumerical<uint32_t>(value_list, result);
    case RosValue::Type::uint64:
        return TransformNumerical<uint64_t>(value_list, result);
    case RosValue::Type::int8:
        return TransformNumerical<int8_t>(value_list, result); 
    case RosValue::Type::int16 :
        return TransformNumerical<int16_t>(value_list, result); 
    case RosValue::Type::int32:
        return TransformNumerical<int32_t>(value_list, result);
    case RosValue::Type::int64:
        return TransformNumerical<int64_t>(value_list, result);
    case RosValue::Type::float32:
        return TransformNumerical<float_t>(value_list, result);
    case RosValue::Type::float64:
        return TransformNumerical<double_t>(value_list, result);
    case RosValue::Type::ros_time: 
        return TransformTime(value_list, result); 
    case RosValue::Type::ros_duration: 
        return TransformInterval(value_list, result);
    case RosValue::Type::string:
        return TransformString(value_list, result);
    case RosValue::Type::ros_bool:
        return TransformNumerical<bool>(value_list, result); 
    case RosValue::Type::primitive_array:
    case RosValue::Type::array:
        if (result.GetType() == LogicalTypeId::ARRAY) {
            return TransformArrayToArray(value_list, result); 
        } else {
            return TransformArrayToList(value_list, result); 
        }
    case RosValue::Type::object: 
        return TransformObjectToStruct(value_list, result); 
    }
    return false; 
}

template <class T>
static bool TransformNumerical( const vector<RosValue::Pointer>& value_list, Vector &result) {
	auto data = FlatVector::GetData<T>(result);
	auto &validity = FlatVector::Validity(result);
    validity.SetAllValid(value_list.size());
    
    // Potentially catch exceptions here and set validity
	for (size_t i = 0; i < value_list.size(); i++) {
        data[i] = value_list[i]->as<T>(); 
	}
	return true;
}
static bool TransformTime(const vector<RosValue::Pointer>& value_list, Vector& result) {
    auto data = FlatVector::GetData<timestamp_t>(result);
	auto &validity = FlatVector::Validity(result);
    validity.SetAllValid(value_list.size()); 

    for (size_t i = 0; i < value_list.size(); i++) {
        data[i] = Timestamp::FromEpochNanoSeconds(value_list[i]->as<RosValue::ros_time_t>().to_nsec()); 
	}
	return true;
}

static bool TransformTime(const vector<RosValue::ros_time_t>& value_list, Vector& result) {
    auto data = FlatVector::GetData<timestamp_t>(result);
	auto &validity = FlatVector::Validity(result);
    validity.SetAllValid(value_list.size()); 

    for (size_t i = 0; i < value_list.size(); i++) {
        data[i] = Timestamp::FromEpochNanoSeconds(value_list[i].to_nsec()); 
	}
	return true;
}

static bool TransformInterval(const vector<RosValue::Pointer>& value_list, Vector& result) {
    auto data = FlatVector::GetData<interval_t>(result);
	auto &validity = FlatVector::Validity(result);
    validity.SetAllValid(value_list.size()); 

    for (size_t i = 0; i < value_list.size(); i++) {
        data[i] = Interval::FromMicro(static_cast<long long>(value_list[i]->as<RosValue::ros_time_t>().to_nsec()) * Interval::NANOS_PER_MICRO); 
	}
	return true;
}

static bool TransformString(const vector<RosValue::Pointer>& value_list, Vector& result) {
    auto data = FlatVector::GetData<string_t>(result);
	auto &validity = FlatVector::Validity(result);
    validity.SetAllValid(value_list.size());

    // Potentially catch exceptions here and set validity
	for (size_t i = 0; i < value_list.size(); i++) {
        data[i] = StringVector::AddString(result, value_list[i]->as<std::string>()); 
	}
	return true;
}

static bool TransformArrayToArray(const vector<RosValue::Pointer>& value_list, Vector& result) {
    size_t count = value_list.size(); 

	// Initialize array vector
	auto &result_validity = FlatVector::Validity(result);
	auto array_size = ArrayType::GetSize(result.GetType());
	auto child_count = count * array_size;

    result_validity.SetAllValid(count); 

    auto nested_values = vector<RosValue::Pointer>();
    nested_values.reserve(child_count);  

	for (idx_t i = 0; i < count; i++) {
        auto sub_values = value_list[i]->getValues();
        std::copy(sub_values.begin(), sub_values.end(), std::back_inserter(nested_values)); 
    }
    return TransformValues(nested_values, ArrayVector::GetEntry(result)); 
}

static bool TransformArrayToList(const vector<RosValue::Pointer>& value_list, Vector& result) {
    size_t count = value_list.size(); 
    auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &list_validity = FlatVector::Validity(result);

    list_validity.SetAllValid(count);
	idx_t offset = 0;

    // loop through values once and get sizes and offsets
	for (idx_t i = 0; i < count; i++) {
		auto &entry = list_entries[i];
		entry.offset = offset;
		entry.length = value_list[i]->size();
		offset += entry.length;
	}

	ListVector::SetListSize(result, offset);
	ListVector::Reserve(result, offset);

    auto nested_values = vector<RosValue::Pointer>();
    nested_values.reserve(offset);

    for (idx_t i = 0; i < count; i++) {
        auto values = value_list[i]->getValues();
        std::copy(values.begin(), values.end(), std::back_inserter(nested_values)); 
    }
    return TransformValues(nested_values, ListVector::GetEntry(result)); 
}

using EmbeddedValueMapType = unordered_map<std::string, vector<RosValue::Pointer> >; 
using ObjectIndexType = pair<string, size_t>;

static bool TransformObjectToStruct(const vector<RosValue::Pointer>& value_list, Vector& result) {
	// Get child vectors and names
	auto &child_vs = StructVector::GetEntries(result);

	vector<string> child_names;
	vector<Vector *> child_vectors;

	child_names.reserve(child_vs.size());
	child_vectors.reserve(child_vs.size());


    // Iterate over schema to get child vector structures
    for (idx_t child_i = 0; child_i < child_vs.size(); child_i++) {
		child_names.push_back(StructType::GetChildName(result.GetType(), child_i));
		child_vectors.push_back(child_vs[child_i].get());
	}

    // Iterate over object fields to create a value array index
    unordered_map<std::string, vector<RosValue::Pointer> > embedded_value_map; 
    auto object_indices = value_list[0]->getObjectIndices();

    for (auto object_idx: object_indices) {
        auto& current_value_vector = embedded_value_map[object_idx.first];
        current_value_vector.reserve(value_list.size()); 

        for (idx_t value_i = 0; value_i < value_list.size(); value_i++) {
            current_value_vector.emplace_back(value_list[value_i]->at(object_idx.second));
        }
    }
    for (idx_t i = 0; i < child_names.size(); i++) {
        auto name = child_names[i]; 
        TransformValues(embedded_value_map[name], *child_vectors[i]);
    }
    return true; 
}

static void TransformHeaderField(EmbeddedValueMapType& embedded_value_map, 
                                 const vector<RosValue::Pointer>& message_values, 
                                 const unordered_map<string,size_t>& header_indices, 
                                 const ObjectIndexType& object_idx) {
    for (auto header_idx: header_indices) {
        auto& header_value_vector = embedded_value_map[HEADER_COL_PREFIX + header_idx.first];
        header_value_vector.reserve(message_values.size()); 
        for (auto& value: message_values) {
            header_value_vector.emplace_back(value->at(object_idx.second)->at(header_idx.second));
        }
    }
}

static void TransformFields(EmbeddedValueMapType& embedded_value_map, 
                            const vector<RosValue::Pointer>& message_values, 
                            const ObjectIndexType& object_idx) {
    auto& current_value_vector = embedded_value_map[object_idx.first];
    current_value_vector.reserve(message_values.size()); 

    for (auto& value: message_values) {
        current_value_vector.emplace_back(value->at(object_idx.second));
    }
}

bool RosTransform::TransformMessages(const RosReaderOptions& options, 
                                     const vector<RosValue::Pointer>& message_values, 
                                     const vector<RosValue::ros_time_t>& message_rx_time,
                                     const vector<string>& col_names,  
                                     vector<Vector>& result) {

    // Iterate over object fields to create a value array index
    // TODO: Clean up this code and make more clear; 
    unordered_map<std::string, vector<RosValue::Pointer> > embedded_value_map; 
    auto object_indices = message_values[0]->getObjectIndices();
    if (options.split_header) {
        auto header_indices = message_values[0]->at("header")->getObjectIndices(); 
        for (auto object_idx: object_indices) {
            if(object_idx.first == "header") {
                TransformHeaderField(embedded_value_map, message_values, header_indices, object_idx); 
            } else {
                TransformFields(embedded_value_map, message_values, object_idx); 
            }
        }
    } else {
        for (auto object_idx: object_indices) {
            TransformFields(embedded_value_map, message_values, object_idx); 
        }   
    }
    for (idx_t i = 0; i < col_names.size(); i++) {
        auto name = col_names[i];
        if (name == options.rx_timestamp_col) {
            TransformTime(message_rx_time, result[i]);
        } else {
            TransformValues(embedded_value_map[name], result[i]);
        }
    }
    return true; 
}

bool RosTransform::TransformTypeToSchema(const RosMsgTypes::MsgDef& msg_def, RosReaderOptions& options, vector<string>& names, vector<LogicalType>& types) {
    if (options.rx_timestamp_col != "") {
        names.push_back(options.rx_timestamp_col); 
        types.push_back(LogicalType::TIMESTAMP_NS);
    }    
    
    // This flag is set to true if we successfully split the header field
    // In this case we can ignore the header field when splitting the main message fields 
    bool can_split_header = false; 

    // If spilt header option is enabled and there is a header, add header fields to schema
    // Lets dynamically add header fields to shema 
    if (options.split_header && (msg_def.fieldIndexes()->count("header") > 0)) {
        auto header_idx = msg_def.fieldIndexes()->at("header");
        auto header_mem = msg_def.members().at(header_idx); 

        if ( header_mem.index() == 0) {
            auto header = std::get<RosMsgTypes::FieldDef>(header_mem); 
            if (header.type() ==  RosValue::Type::object) {
                can_split_header = true; 
                for (auto member : header.typeDefinition().members()) {
                    if (member.index() == 0) {
                        auto field = std::get<RosMsgTypes::FieldDef>(member); 
                        names.push_back("header." + field.name()); 
                        types.push_back(RosTransform::ConvertRosFieldType(field)); 
                    }
                }
            }
        } 
    }
    // Convert each ros message field into schema 
    for (auto member : msg_def.members()) {
        if (member.index() == 0) { 
            auto field = std::get<RosMsgTypes::FieldDef>(member); 
            // Ignore header field if we've already split it. 
            if (!(can_split_header && field.name() == "header")) {
                 names.push_back(field.name()); 
                 types.push_back(RosTransform::ConvertRosFieldType(field)); 
            }
        }
    } 
    options.split_header = can_split_header; 
    return true; 
}

}