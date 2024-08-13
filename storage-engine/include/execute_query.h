#include <fcntl.h>
#include <storage_engine.h>
#include <unistd.h>

#include <cstddef>
#include <vector>

#include "absl/status/statusor.h"

absl::Status counting_execute_query(
    StorageEngine& storage_engine,
    const std::vector<StorageEngine::BlockId>& col_a,
    const std::vector<StorageEngine::BlockId>& col_b,
    int upper_bound,
    std::vector<size_t>& cnt
);
