#pragma once

#include <cstring>

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include <duckdb/common/string.hpp>
#include <duckdb/common/unordered_map.hpp>
#include <duckdb/common/vector.hpp>
#endif

#include <bzlib.h>

#include "ros_value.hpp"
#include "decompression.hpp"

namespace duckdb {

struct RosBagTypes {
  struct connection_data_t {
    string topic;
    string type;
    string scope;
    string md5sum;
    string message_definition;
    string callerid;
    bool latching = false;
    size_t message_count = 0;

    bool operator==(const connection_data_t &other) const {
      return other.topic == topic &&
          other.type == type &&
          other.md5sum == md5sum &&
          other.callerid == callerid &&
          other.latching == latching;
    }
  };

  struct record_t {
    uint32_t header_len;
    const char *header;
    uint32_t data_len;
    const char *data;
  };


  enum class op {
    BAG_HEADER = 0x03,
    CHUNK = 0x05,
    CONNECTION = 0x07,
    MESSAGE_DATA = 0x02,
    INDEX_DATA = 0x04,
    CHUNK_INFO = 0x06,
    UNSET = 0xff,
  };

  struct chunk_info_t {
    uint64_t chunk_pos;
    RosValue::ros_time_t start_time;
    RosValue::ros_time_t end_time;
    uint32_t message_count = 0;
    uint32_t connection_count = 0;
  };

  struct chunk_t {
    uint64_t offset = 0;
    chunk_info_t info;
    string compression;
    uint32_t uncompressed_size = 0;
    uint32_t header_len = 0; 
    uint32_t data_len = 0; 

    void decompress(const char* src, char *dst) const {
      if (compression == "lz4") {
        decompressLz4Chunk(src, dst);
      } else if (compression == "bz2") {
        decompressBz2Chunk(src, dst);
      } else if (compression == "none") {
        memcpy(dst, src, uncompressed_size);
      }
    }

    void decompressLz4Chunk(const char* src, char *dst) const {
      size_t src_bytes_left = data_len;
      size_t dst_bytes_left = uncompressed_size;

      while (dst_bytes_left && src_bytes_left) {
        size_t src_bytes_read = src_bytes_left;
        size_t dst_bytes_written = dst_bytes_left;
        auto& lz4_ctx = Lz4DecompressionCtx::getInstance();
        const size_t ret = LZ4F_decompress(lz4_ctx.context(), dst, &dst_bytes_written, src, &src_bytes_read, nullptr);
        if (LZ4F_isError(ret)) {
          throw std::runtime_error("chunk::decompress: lz4 decompression returned " + std::to_string(ret) + ", expected "
                                       + std::to_string(src_bytes_read));
        }

        src_bytes_left -= src_bytes_read;
        dst_bytes_left -= dst_bytes_written;
      }

      if (src_bytes_left || dst_bytes_left) {
        throw std::runtime_error("chunk::decompress: lz4 decompression left " + std::to_string(src_bytes_left) + "/"
                                     + std::to_string(dst_bytes_left) + " bytes in buffer");
      }
    };

    void decompressBz2Chunk(const char* src, char *dst) const {
      unsigned int dst_bytes_left = uncompressed_size;
      char * source = const_cast<char *>(src);
      const auto r = BZ2_bzBuffToBuffDecompress(dst, &dst_bytes_left, source, data_len, 0,0);
      if (r != BZ_OK) {
        throw std::runtime_error("Failed decompress bz2 chunk, bz2 error code: " + std::to_string(r));
      }
    }
  };

  struct connection_record_t {
    uint32_t id;
    vector<const chunk_t&> blocks;
    string topic;
    connection_data_t data;
  };
};
}