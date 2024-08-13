#include <execute_query.h>

#include <cassert>
#include <cstddef>
#include <future>
#include <iostream>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/barrier.h"
#include "storage_engine.h"


absl::Status counting_execute_query(
    StorageEngine& storage_engine,
    const std::vector<StorageEngine::BlockId>& col_a,
    const std::vector<StorageEngine::BlockId>& col_b, int upper_bound,
    std::vector<size_t>& cnt) {
    const size_t block_size = storage_engine.get_block_size();
    const size_t block_value_count = block_size / sizeof(int);

    std::vector<bool> block_mask;
    int sum = 0;

    for (int t = 0; t < col_a.size(); ++t) {
        const StorageEngine::BlockId col_a_block_id = col_a[t];
        const StorageEngine::BlockId col_b_block_id = col_b[t];

        const auto get_block_a_res = storage_engine.get_block(col_a_block_id);
        // const auto count_result =
        // storage_engine.counting_get_block(col_a_block_id, cnt);
        // assert(count_result.ok() && "Counting failed");
        if (!get_block_a_res.ok()) return get_block_a_res.status();
        const auto& col_a_block_reader = *get_block_a_res;
        block_mask.assign(block_value_count, false);

        bool at_least_one_true = false;
        for (int i = 0; i < block_value_count; ++i) {
            block_mask[i] = (col_a_block_reader.read_int(i) < upper_bound);
            at_least_one_true |= block_mask[i];
        }
        if (at_least_one_true) {
            const auto count_result =
                storage_engine.counting_get_block(col_b_block_id, cnt);
            assert(count_result.ok());
        }
    }

    return absl::OkStatus();
}
