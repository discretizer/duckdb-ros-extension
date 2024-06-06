#include "ros_reader_options.hpp"
namespace duckdb {
void RosReaderOptions::AddParameters(TableFunction& table_function) {
    table_function.named_parameters["topic"] = LogicalType::VARCHAR; 
    table_function.named_parameters["split_header"] = LogicalType::BOOLEAN;
	table_function.named_parameters["rx_timestamp_col"] = LogicalType::VARCHAR;
}

void RosReaderOptions::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<string>(100, "topic", topic);
	serializer.WritePropertyWithDefault<bool>(101, "split_header", split_header, true);
    serializer.WritePropertyWithDefault<string>(102, "rx_timestamp_col", rx_timestamp_col, "rx_timestamp"); 
	serializer.WriteProperty<MultiFileReaderOptions>(103, "file_options", file_options);
}

RosReaderOptions RosReaderOptions::Deserialize(Deserializer& deserializer) {
    RosReaderOptions result; 
    deserializer.ReadProperty<string>(100, "topic", result.topic); 
    deserializer.ReadPropertyWithDefault<bool>(101, "split_header", result.split_header, true); 
    deserializer.ReadPropertyWithDefault<string>(102, "rx_timestamp_col", result.rx_timestamp_col, "rx_timestamp");
    deserializer.ReadProperty<MultiFileReaderOptions>(103, "file_options", result.file_options); 

    return result; 
} 
}