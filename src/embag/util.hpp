#pragma once

#include <boost/iostreams/stream.hpp>

namespace Embag {
#if __cplusplus < 201402L
// std::make_unique is not available in c++11 :(
template<typename T, typename... Args>
std::unique_ptr<T> make_unique(Args &&... args) {
  return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

using message_stream = boost::iostreams::stream<boost::iostreams::array_source>;
#else
using std::make_unique;
#endif
}