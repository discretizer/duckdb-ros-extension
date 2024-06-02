#pragma once

#include <cstdint>
#include <cstring>
#include <variant>

#include <duckdb.hpp>

#ifndef DUCKDB_AMALGAMATION
#include <duckdb/common/string.hpp>
#include <duckdb/common/unordered_map.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/common/pair.hpp>
#include <duckdb/common/shared_ptr.hpp>
#endif

#include "span.hpp"

namespace duckdb {

class RosValue {
 public:
  class Pointer;

  struct ros_value_list_t {
    weak_ptr<vector<RosValue>> base;
    size_t offset;
    size_t length;

    const Pointer at(size_t index) const;
  };

  enum class Type {
    ros_bool,
    int8,
    uint8,
    int16,
    uint16,
    int32,
    uint32,
    int64,
    uint64,
    float32,
    float64,
    string,
    ros_time,
    ros_duration,

    // Custom types
    object,
    array,
    primitive_array,
  };
  static size_t primitiveTypeToSize(const Type type);
  static string primitiveTypeToFormat(const Type type);

 private:
  template<class ChildType>
  class TimeValue {
   public:
    uint32_t secs = 0;
    uint32_t nsecs = 0;

    double to_sec() const {
      return double(secs) + double(nsecs) / 1e9;
    }

    long to_nsec() const {
      return long(secs) * long(1e9) + long(nsecs);
    }

    TimeValue() {};
    TimeValue(const uint32_t secs, const uint32_t nsecs) : secs(secs), nsecs(nsecs) {}

    bool operator==(const ChildType &other) const {
      return secs == other.secs && nsecs == other.nsecs;
    }

    bool operator!=(const ChildType &other) const {
      return secs != other.secs || nsecs != other.nsecs;
    }

    bool operator<(const ChildType &other) const {
      return secs < other.secs || (secs == other.secs && nsecs < other.nsecs);
    }

    bool operator<=(const ChildType &other) const {
      return secs < other.secs || (secs == other.secs && nsecs <= other.nsecs);
    }

    bool operator>(const ChildType &other) const {
      return secs > other.secs || (secs == other.secs && nsecs > other.nsecs);
    }

    bool operator>=(const ChildType &other) const {
      return secs > other.secs || (secs == other.secs && nsecs >= other.nsecs);
    }
  };

 public:
  class ros_time_t : public TimeValue<ros_time_t> {
    using TimeValue<ros_time_t>::TimeValue;
  };

  struct ros_duration_t : public TimeValue<ros_duration_t> {
    using TimeValue<ros_duration_t>::TimeValue;
  };

  Type getType() const {
    return type_;
  }

  Type getElementType() const;

 private:
  template<class ReturnType, class IndexType, class ChildIteratorType>
  class const_iterator_base {
   public:
    bool operator==(const ChildIteratorType& other) const {
      return index_ == other.index_;
    }

    bool operator!=(const ChildIteratorType& other) const {
      return !(*this == other);
    }

    ChildIteratorType& operator++() {
      ++index_;
      return *((ChildIteratorType*) this);
    }

    ChildIteratorType operator++(int) {
      return {value_, index_++};
    }
   protected:
    const_iterator_base(const RosValue& value, IndexType index)
      : value_(value)
      , index_(index)
    {
    }

    const RosValue& value_;
    IndexType index_;
  };

 public:
  // Iterator that implements an InputIterator over the children
  // of RosValues that are of type object or array
  template<class ReturnType, class IndexType>
  class const_iterator;

  template<class ReturnType>
  class const_iterator<ReturnType, size_t> : public const_iterator_base<ReturnType, size_t, const_iterator<ReturnType, size_t>> {
   public:
    const_iterator(const RosValue& value, size_t index)
      : const_iterator_base<ReturnType, size_t, const_iterator<ReturnType, size_t>>(value, index)
    {
      if (value.type_ != Type::object && value.type_ != Type::array && value.type_ != Type::primitive_array) {
        throw std::runtime_error("Cannot iterate the values of a non-object or non-array RosValue");
      }
    }

    const ReturnType operator*() const {
      return this->value_.at(this->index_);
    }
  };

  template<class ReturnType>
  class const_iterator<ReturnType, unordered_map<std::string, size_t>::const_iterator> : public const_iterator_base<ReturnType, std::unordered_map<std::string, size_t>::const_iterator, const_iterator<ReturnType, std::unordered_map<std::string, size_t>::const_iterator>> {
   public:
    const_iterator(const RosValue& value, std::unordered_map<std::string, size_t>::const_iterator index)
      : const_iterator_base<ReturnType, std::unordered_map<std::string, size_t>::const_iterator, const_iterator<ReturnType, std::unordered_map<std::string, size_t>::const_iterator>>(value, index)
    {
      if (value.type_ != Type::object) {
        throw std::runtime_error("Cannot iterate the keys or key/value pairs of an non-object RosValue");
      }
    }

    const ReturnType operator*() const;
  };

  template<class IteratorReturnType>
  const_iterator<IteratorReturnType, size_t> beginValues() const {
    return RosValue::const_iterator<IteratorReturnType, size_t>(*this, 0);
  }
  template<class IteratorReturnType>
  const_iterator<IteratorReturnType, size_t> endValues() const {
    return RosValue::const_iterator<IteratorReturnType, size_t>(*this, this->size());
  }
  template<class IteratorReturnType>
  const_iterator<IteratorReturnType, std::unordered_map<std::string, size_t>::const_iterator> beginItems() const {
    if (type_ != Type::object) {
      throw std::runtime_error("Cannot iterate over the items of a RosValue that is not an object");
    }

    return RosValue::const_iterator<IteratorReturnType, std::unordered_map<std::string, size_t>::const_iterator>(*this, std::get<object_info_t>(info_).field_indexes->cbegin());
  }
  template<class IteratorReturnType>
  const_iterator<IteratorReturnType, std::unordered_map<std::string, size_t>::const_iterator> endItems() const {
    if (type_ != Type::object) {
      throw std::runtime_error("Cannot iterate over the items of a RosValue that is not an object");
    }

    return RosValue::const_iterator<IteratorReturnType, std::unordered_map<std::string, size_t>::const_iterator>(*this, std::get<object_info_t>(info_).field_indexes->cend());
  }

 private:
  struct _array_identifier {};
 public:
  RosValue(const Type type, std::string_view message_buffer, const size_t offset)
    : type_(type)
    , info_(std::in_place_type<primitive_info_t>, offset, message_buffer )
  {
    if (type_ == Type::object || type_ == Type::array || type_ == Type::primitive_array) {
      throw std::runtime_error("Cannot create an object or array with this constructor");
    }
  }
  RosValue(const Type type)
    : type_(type)
    , info_(std::in_place_type<primitive_info_t>,  0, nullptr)
  {
    if (type_ == Type::object || type_ == Type::array || type_ == Type::primitive_array) {
      throw std::runtime_error("Cannot create an object or array with this constructor");
    }
  }
  RosValue(const shared_ptr<unordered_map<string, size_t>>& field_indexes)
    : type_(Type::object)
    , info_(std::in_place_type<object_info_t>, field_indexes)
  {
  }
  RosValue(const _array_identifier &i)
    : type_(Type::array)
    , info_(std::in_place_type<array_info_t>)
  {
  }
  RosValue(const Type element_type, std::string_view message_buffer)
    : type_(Type::primitive_array)
    , info_(std::in_place_type<primitive_array_info_t>, element_type, message_buffer)
  {
  }

  // Convenience accessors
  const Pointer operator()(const std::string &key) const;
  const Pointer operator[](const std::string &key) const;
  const Pointer operator[](const size_t idx) const;
  const Pointer get(const string &key) const;
  const Pointer at(size_t idx) const;
  const Pointer at(const string &key) const;

  template<typename T>
  const T &getValue(const string &key) const;

  template<typename T>
  const T as() const {
    if (type_ == Type::object || type_ == Type::array) {
      throw std::runtime_error("Value cannot be an object or array for as");
    }

    // TODO: Add check that the underlying type aligns with T
    return getPrimitive<T>();
  }

  bool has(const std::string &key) const {
    if (type_ != Type::object) {
      throw std::runtime_error("Value is not an object");
    }

    return std::get<object_info_t>(info_).field_indexes->count(key);
  }

  size_t size() const {
    if (type_ == Type::array || type_ == Type::object) {
      return getChildren().length;
    } else if (type_ == Type::primitive_array) {
      return std::get<primitive_array_info_t>(info_).length;
    } else {
      throw std::runtime_error("Value is not an array or an object");
    }
  }

  // Provides access to the underlying buffer for a RosValue of type primitive_array
  // The life of the buffer is only guaranteed to live as long as the RosValuePointer does,
  // and as a result this should be used with great caution.
  const void* getPrimitiveArrayRosValueBuffer() const;
  size_t getPrimitiveArrayRosValueBufferSize() const;

  unordered_map<string, Pointer> getObjects() const;
  const std::unordered_map<std::string, size_t>& getObjectIndices() const;
  vector<Pointer> getValues() const;

  string toString(const string &path = "") const;
  void print(const string &path = "") const;

  // Used for accessor template resolution
  template<typename T>
  struct identity { typedef T type; };

 private:
  struct primitive_info_t {
    primitive_info_t(size_t off, std::string_view buf)
      : offset(off)
      , message_buffer(buf)
    {}
    size_t offset; 
    std::string_view message_buffer;
  };

  struct primitive_array_info_t {
    primitive_array_info_t(const Type element_type, std::string_view message_buffer)
      : element_type(element_type)
      , message_buffer(message_buffer)
    {
    }

    Type element_type;
    size_t offset;
    size_t length;
    std::string_view message_buffer;
  };

  struct array_info_t {
    ros_value_list_t children;
  };

  struct object_info_t {
    object_info_t(shared_ptr<unordered_map<string, size_t>> field_idx) 
      : field_indexes(field_idx) 
      {}
    ros_value_list_t children;
    shared_ptr<unordered_map<string, size_t>> field_indexes;
  };

  Type type_;
  std::variant<primitive_info_t, primitive_array_info_t, array_info_t, object_info_t> info_; 
  
  template<typename T>
  const T& getPrimitive() const {
    return reinterpret_cast<const T&>(std::get<primitive_info_t>(info_).message_buffer.at(std::get<primitive_info_t>(info_).offset));
  }

  const ros_value_list_t& getChildren() const {
    switch(type_) {
      case Type::object:
        return std::get<object_info_t>(info_).children;
      case Type::array:
        return std::get<array_info_t>(info_).children;
      default:
        throw std::runtime_error("Cannot getChildren of a RosValue that is not an object or array");
    }
  }

  friend class MessageParser;
};

class RosValue::Pointer {
 private:
  struct vector_based_value_info_t {
    shared_ptr<vector<RosValue>> base;
    size_t index;
  };
  std::variant<RosValue, vector_based_value_info_t> info_;

 public:
  Pointer()
    : info_(vector_based_value_info_t({nullptr, 0}))
  {
  }

  Pointer(const weak_ptr<vector<RosValue>>& base)
    : Pointer(base, 0)
  {
  }

  Pointer(const weak_ptr<vector<RosValue>>& base, size_t index)
    : info_(vector_based_value_info_t({base.lock(), index}))
  {
  }

  Pointer(const shared_ptr<vector<RosValue>>& base, size_t index)
    : info_(vector_based_value_info_t({base, index}))
  {
  }

  Pointer(const RosValue::Type type, std::string_view message_buffer, const size_t offset)
    : info_(RosValue(type, message_buffer, offset))
  {
  }

  const Pointer operator()(const string &key) const {
    return (**this)(key);
  }

  const Pointer operator[](const string &key) const {
    return (**this)[key];
  }

  const Pointer operator[](const size_t idx) const {
    return (**this)[idx];
  }

  const RosValue* operator->() const {
    return &**this;
  }

 private:
  const RosValue& operator*() const {
    if (info_.index() == 0) {
      return std::get<RosValue>(info_);
    } else {
      vector_based_value_info_t info = std::get<vector_based_value_info_t>(info_);
      return info.base->at(info.index);
    }
  }
};

template<>
const string RosValue::as<std::string>() const;

template<>
const string& RosValue::const_iterator<const string&, unordered_map<string, size_t>::const_iterator>::operator*() const;

template<>
const pair<const string&, const RosValue&> RosValue::const_iterator<const pair<const string&, const RosValue&>, unordered_map<string, size_t>::const_iterator>::operator*() const;

}