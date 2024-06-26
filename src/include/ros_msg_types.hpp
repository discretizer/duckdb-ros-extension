#pragma once

#include <duckdb.hpp>

#ifndef DUCKDB_AMALGAMATION

#include <duckdb/common/unordered_map.hpp>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/common/vector.hpp>
#endif 

#include "ros_value.hpp"

namespace duckdb {

class RosMsgTypes{
 public:
  class EmbeddedMsgDef;

  // Schema stuff
  // TODO: move this stuff elsewhere?
  typedef unordered_map<string, const RosValue::Type> primitive_type_map_t;
  class FieldDef {
   public:
    const static primitive_type_map_t primitive_type_map;
    static size_t typeToSize(const RosValue::Type type);

    struct parseable_info_t {
      string type_name;
      int32_t array_size;
      string field_name;
    };

    FieldDef(parseable_info_t parsed_info)
      : parsed_info_(parsed_info)
      , type_definition_(nullptr)
    {
      if (primitive_type_map.count(parsed_info_.type_name)) {
        type_ = primitive_type_map.at(parsed_info_.type_name);
        if (type_ != RosValue::Type::string) {
          // Cache the size of the type for quicker access.
          type_size_ = RosValue::primitiveTypeToSize(type_);
        }
      } else {
        type_ = RosValue::Type::object;
      }
    }

    const string& typeName() const {
      return parsed_info_.type_name;
    }

    int32_t arraySize() const {
      return parsed_info_.array_size;
    }

    const string& name() const {
      return parsed_info_.field_name;
    }

    RosValue::Type type() const {
      return type_;
    }

    size_t typeSize() const {
      if (type_ == RosValue::Type::object || type_ == RosValue::Type::string) {
        throw std::runtime_error("The size of an object or string is not statically known!");
      }

      return type_size_;
    }

    void setTypeDefinition(const unordered_map<string, EmbeddedMsgDef> &definition_map, const string &scope) {
      if (type_ != RosValue::Type::object) {
        throw std::runtime_error("Cannnot set the typeDefinition for non-object types");
      }

      if (definition_map.count(parsed_info_.type_name)) {
        type_definition_ = &definition_map.at(parsed_info_.type_name);
      }

      // ROS allows a type to lack its scope when referenced
      const auto scoped_name = scope + '/' + parsed_info_.type_name;
      if (definition_map.count(scoped_name)) {
        type_definition_ = &definition_map.at(scoped_name);
      }

      if (type_definition_ == nullptr) {
        throw std::runtime_error("Unable to find embedded type: " + parsed_info_.type_name + " in scope " + scope);
      }
    }

    const EmbeddedMsgDef& typeDefinition() const {
      if (type_ != RosValue::Type::object) {
        throw std::runtime_error("Non object types do not have a typeDefinition");
      }

      return *type_definition_;
    }

    const parseable_info_t parsed_info_;

    // While the primitive_type_map_ contains this information, its fairly unperformant.
    // To maintain performance, we cache this information in each instance of the class.
    // If this field is an array, holds the type of the items within the array.
    RosValue::Type type_;
    // If this field is an object or a string, the size will be 0.
    size_t type_size_ = 0;

    // TODO: This can be stored in union with the size_t to reduce space
    // If this is an object, cache the associated ros_embedded_msg_def
    const EmbeddedMsgDef* type_definition_ = nullptr;
  };

  struct ConstantDef {
    string type_name;
    string constant_name;
    string value;
  };

  typedef boost::variant2::variant<FieldDef, ConstantDef> MemberDef;
  typedef boost::variant2::variant<FieldDef::parseable_info_t, ConstantDef> member_parseable_info_t;

  class BaseMsgDef {
   public:
    struct parseable_info_t {
      vector<member_parseable_info_t> members;
    };

    BaseMsgDef(const parseable_info_t& parsed_info, const string& name)
      : name_(name)
    {
      size_t num_fields = 0;
      for (const auto &member : parsed_info.members) {
        if (member.index() == 0) {
          ++num_fields;
        }
      }

      members_.reserve(parsed_info.members.size());
      field_indexes_ = make_shared_ptr<std::unordered_map<std::string, size_t>>();
      field_indexes_->reserve(num_fields);
      size_t field_num = 0;
      for (const auto& member : parsed_info.members) {
        if (member.index() == 0) {
          members_.emplace_back(boost::variant2::get<FieldDef::parseable_info_t>(member));
          field_indexes_->emplace(boost::variant2::get<FieldDef::parseable_info_t>(member).field_name, field_num++);
        } else {
          members_.emplace_back(boost::variant2::get<ConstantDef>(member));
        }
      }

      // TODO: make this less dumb
      const size_t slash_pos = name_.find_first_of('/');
      if (slash_pos != std::string::npos) {
        scope_ = name_.substr(0, slash_pos);
      } else {
        scope_ = "";
      }
    }

    static const std::string& getMemberName(const MemberDef &member) {
      if ( member.index() == 0) {
        return boost::variant2::get<FieldDef>(member).name();
      } else {
        return boost::variant2::get<ConstantDef>(member).constant_name;
      }
    }

    const shared_ptr<std::unordered_map<std::string, size_t>>& fieldIndexes() const {
      return field_indexes_;
    }

    const std::vector<MemberDef>& members() const {
      return members_;
    };

    const std::string& name() const {
      return name_;
    }

    const std::string& scope() const {
      return scope_;
    }

    void initializeFieldTypeDefinitions(const std::unordered_map<std::string, EmbeddedMsgDef>& definition_map) {
      for (auto& member : members_) {
        if (member.index() == 0) {
          auto& field = boost::variant2::get<FieldDef>(member);
          if (field.type() == RosValue::Type::object) {
            field.setTypeDefinition(definition_map, scope_);
          }
        }
      }
    }

   private:
    shared_ptr<std::unordered_map<std::string, size_t>> field_indexes_;
    vector<MemberDef> members_;
    const string name_;
    string scope_;
  };

  class EmbeddedMsgDef : public BaseMsgDef {
   public:
    struct parseable_info_t : BaseMsgDef::parseable_info_t {
      std::string type_name;
    };

    EmbeddedMsgDef(parseable_info_t parsed_info)
      : BaseMsgDef(parsed_info, parsed_info.type_name)
    {
    }
  };

  class MsgDef : public BaseMsgDef {
   public:
    struct parseable_info_t : BaseMsgDef::parseable_info_t {
      std::vector<EmbeddedMsgDef::parseable_info_t> embedded_definitions;
    };

    MsgDef(parseable_info_t parsed_info, const std::string& name)
      : BaseMsgDef(parsed_info, name)
      , embedded_definition_map_(parsed_info.embedded_definitions.size())
    {
      for (const auto &embedded_definition: parsed_info.embedded_definitions) {
        embedded_definition_map_.emplace(embedded_definition.type_name, embedded_definition);
      }

      initializeFieldTypeDefinitions(embedded_definition_map_);
      for (auto &embedded_definition_kv: embedded_definition_map_) {
        embedded_definition_kv.second.initializeFieldTypeDefinitions(embedded_definition_map_);
      }
    }

   private:
    unordered_map<string, EmbeddedMsgDef> embedded_definition_map_;
  };
};

}