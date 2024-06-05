#pragma once 

#include <duckdb.hpp>

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/file_system.hpp"
#endif

#include "ros_bag_types.hpp"

#include <boost/utility/string_view.hpp> 

namespace duckdb {

constexpr idx_t RECORD_HEADER_SIZE = 1024ULL * 4ULL;
constexpr idx_t RECORD_DATA_SIZE = 1024ULL * 16ULL; 

class RosRecordParser{
public:  
    RosRecordParser(Allocator& a): 
        allocator(a),
        header_len(0UL), 
        header(a.Allocate(RECORD_DATA_SIZE)),
        data_len(0UL), 
        data(a.Allocate(RECORD_HEADER_SIZE))
    { 
    }
    void Read(FileHandle& file_handler, bool header_only=false) {
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
            if (data_len > data.GetSize()) {
                idx_t next_len = NextPowerOfTwo(data_len); 
                data = allocator.Allocate(next_len);
            }
            file_handler.Read(data.get(), data_len); 
        }
    }

    boost::string_view Header() const {
        return boost::string_view(reinterpret_cast<const char *>(header.get()), static_cast<size_t>(header_len)); 
    }

    boost::string_view Data() const {
        return boost::string_view(reinterpret_cast<const char *>(data.get()), static_cast<size_t>(data_len)); 
    }

    uint32_t GetHeaderLength() {return header_len; }
    uint32_t GetDataLength() {return data_len; }

private: 
    Allocator& allocator; 
 
    uint32_t header_len; 
    AllocatedData header;
    
    uint32_t data_len; 
    AllocatedData data;
}; 

class RosBufferedRecordParser {
public: 
    RosBufferedRecordParser(ByteBuffer& data) 
    {
        header_len = data.read<uint32_t>();
        header_ptr = data.ptr; 
        data.inc(header_len); 
        data_len = data.read<uint32_t>(); 
        data_ptr = data.ptr; 
        data.inc(data_len); 
    }

    boost::string_view Header() const {
        return boost::string_view(reinterpret_cast<const char *>(header_ptr), static_cast<size_t>(header_len)); 
    }

    boost::string_view Data() const {
        return boost::string_view(reinterpret_cast<const char *>(data_ptr), static_cast<size_t>(data_len)); 
    }
private: 
    data_ptr_t header_ptr;
    uint32_t header_len; 
    data_ptr_t data_ptr; 
    uint32_t data_len; 
}; 


template <typename T> 
void readField(const boost::string_view& data, T& value) {
    // So there are several subtle issues with just casting the pointer here; 
    // Pointer alignment and other issues prevent just raw (i.e. reinterpret_cast )
    // casting.  Best just to memcpy.
    D_ASSERT(data.size() == sizeof(T)); 
    std::memcpy(&value, data.begin(), data.size());
} 

void readField(const boost::string_view& data, std::string& value) {
    value = data.to_string();
} 

void readField(const boost::string_view& data, RosValue::ros_time_t& time) {
    struct timeval_t {
        uint32_t sec;
        uint32_t nsec; 
    } timeval; 
    readField(data, timeval);

    time = RosValue::ros_time_t(timeval.sec, timeval.nsec); 
}

template <typename T> 
struct field {
public: 
    field(boost::string_view name, T& value): 
        field_name(name), field_ref(value) {
    }
    void apply(const boost::string_view& name, const boost::string_view& data ) {
        if (name == field_name) {
            readField(data, field_ref);
        }
    }

    boost::string_view field_name;
    T& field_ref; 
};


void apply_all(const boost::string_view& name, const boost::string_view& data) 
{ 
// intentionally blank
}

template<class Field, class ... Fargs>
void apply_all(const boost::string_view& name, const boost::string_view& data, Field field, Fargs... fields ) {
    field.apply(name, data); 
    apply_all(name, data, fields...); 
}


template <class ... args>
void readFields(const boost::string_view& data, field<args>&&... fields) {  
  auto current = data.begin(); 

  while (current < data.end()) {
    uint32_t field_len = 0;  
    readField(boost::string_view(current, sizeof(field_len)), field_len); 
    
    current += sizeof(uint32_t);

    boost::string_view buffer(current, field_len); 
    const auto sep = buffer.find('=');

    if (sep == string::npos) {
      throw std::runtime_error("Unable to find '=' in header field - perhaps this bag is corrupt...");
    }

    const auto name = buffer.substr(0, sep);
    const auto data = buffer.substr(sep + 1); 
    // This fun bit of code iterates through the field definitions 
    // and parses out the field of the field name matches. 
    apply_all(name, data, fields...); 

    current += field_len;
  }
}
}