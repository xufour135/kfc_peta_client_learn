#pragma once

#include <cstdint>
#include <cstdlib>
#include <string>

// #include "peta/peta_log.h"

namespace peta {

struct CacheOption {
  CacheOption(const std::string& name, uint32_t capacity, uint64_t ttl_seconds)
      : name(name),
        capacity(capacity),
        bucket_size(capacity),
        ttl(ttl_seconds) {
    if (ttl <= 3) {
        printf("Unexpected short TTL (%lu).", ttl);
    //   PETA_LOG(FATAL) << "Unexpected short TTL (" << ttl << ").";
      abort();
    }
  }

  CacheOption() = delete;

  const std::string name;
  uint32_t capacity;
  uint32_t bucket_size;
  uint64_t ttl;
  uint32_t evict_tolerance_second = 1;
  uint32_t max_update_queue_size = 16;
};

}  // namespace peta
