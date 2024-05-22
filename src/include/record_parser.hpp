#pragma once 

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/file_system.hpp"

#include "ros_bag_types.hpp"

#include <string_view> 

namespace duckdb {

constexpr idx_t RECORD_HEADER_SIZE = 1024ULL * 4ULL;
constexpr idx_t RECORD_DATA_SIZE = 1024ULL * 16ULL; 

class RosRecordParser{
public:  
    RosRecordParser(Allocator& a): 
        allocator(a),
        header_size(0ULL), 
        header(a.Allocate(RECORD_DATA_SIZE)),
        data_size(0ULL), 
        data(a.Allocate(RECORD_HEADER_SIZE))
    { 
    }
    void Read(FileHandle& file_handler, bool header_only=false) {
        uint32_t header_len; 
        uint32_t data_len; 
        file_handler.Read(&header_len, sizeof(header_len)); 
        if (header_len > header.GetSize()) {
            idx_t next_len = NextPowerOfTwo(header_len); 
            header = allocator.Allocate(next_len);
        } 
        file_handler.Read(header.get(), header_len);
        file_handler.Read(&data_len, sizeof(data_len));
        if (header_only) {
            file_handler.Seek(file_handler.SeekPosition() + data_len); 
        } else {
            if (data_len > header.GetSize()) {
                idx_t next_len = NextPowerOfTwo(data_len); 
                data = allocator.Allocate(next_len);
            }
            file_handler.Read(data.get(), data_len); 
        }
    }

    std::string_view Header() const {
        return std::string_view(reinterpret_cast<const char *>(header.get()), static_cast<size_t>(header_size)); 
    }

    std::string_view Data() const {
        return std::string_view(reinterpret_cast<const char *>(data.get()), static_cast<size_t>(data_size)); 
    }

private: 
    Allocator& allocator; 
 
    AllocatedData header;
    idx_t header_size; 

    AllocatedData data;
    idx_t data_size;  
}; 

class RosBufferedRecordParser {
public: 
    RosBufferedRecordParser(ByteBuffer& data) 
    {
        uint32_t header_len; 
        uint32_t data_len; 

        header_len = data.read<uint32_t>();
        header_ptr = data.ptr; 
        data.inc(header_len); 
        data_len = data.read<uint32_t>(); 
        data_ptr = data.ptr; 
        data.inc(data_len); 
    }

    std::string_view Header() const {
        return std::string_view(reinterpret_cast<const char *>(header_ptr), static_cast<size_t>(header_size)); 
    }

    std::string_view Data() const {
        return std::string_view(reinterpret_cast<const char *>(data_ptr), static_cast<size_t>(data_size)); 
    }
private: 
    data_ptr_t header_ptr;
    idx_t header_size; 
    data_ptr_t data_ptr; 
    idx_t data_size; 
}; 


template <typename T> 
std::pair<std::string_view, T&> make_field(std::string_view name, T& val){
    return make_pair<std::string_view, T&>(name, val); 
}

template <typename T> 
void readField(const std::string_view& data, T& value) {
    value = *reinterpret_cast<const T *>(data.data());
} 

void readField(const std::string_view& data, std::string& value) {
    value = data;
} 

void readField(const std::string_view& data, RosValue::ros_time_t& time) {
    
}

template <class ... args>
void readFields(const std::string_view& data, std::pair<std::string_view, args&> ... fields) {  
  auto current = data.begin(); 

  while (current < data.end()) {
    const uint32_t field_len = *(reinterpret_cast<const uint32_t *>(current));
    current += sizeof(uint32_t);

    std::string_view buffer(current, field_len); 
    const auto sep = buffer.find('=');

    if (sep == string::npos) {
      throw std::runtime_error("Unable to find '=' in header field - perhaps this bag is corrupt...");
    }

    const auto name = buffer.substr(0, sep);
    const auto data = buffer.substr(sep + 1); 
    // This fun bit of code iterates through the field definitions 
    // and parses out the field of the field name matches. 
    ([&]
    {
      if (name == fields.first) {
        readField(data, fields.second); 
      }
    } (), ...); 

    current += field_len;
  }
}
}