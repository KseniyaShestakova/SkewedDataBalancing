// DISCLAIMER: this file is only a playground
// it doesn't contain any useful code

#include <storage_engine.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <string.h>
#include "absl/status/statusor.h"
#include "absl/status/status.h"
#include <data_generator.h>

#include <cstddef>
#include <iostream>

#include "data_generator_impl.h"

    constexpr size_t data_size = size_t(2) * 1024 * 1024 * 1024;
static const std::string kStoragePath = "benchmark_store_1";

double blocks_to_be_loaded(size_t block_size, int upper_bound) {
    const size_t block_count = data_size / block_size;
    int n = 10;
    std::vector<float> means;
    std::vector<float> variances;

    means.reserve(n);
    variances.reserve(n);

    for (int i = 0; i < n; ++i) {
        means.emplace_back(4.0 * (i + 1));
        variances.emplace_back(1);
    }

    MixOfNormalDistributions gen(means, variances);
    int** data = gen.generate_blocks<int>(block_count, block_size);

    // for each block verify if it should be loaded
    size_t blocks_to_be_loaded = 0;
    for (size_t i = 0; i < block_count; ++i) {
        bool at_least_one_passes = false;
        for (size_t j = 0; j < block_size / sizeof(int); ++j) {
            at_least_one_passes |= (data[i][j] < upper_bound);
        }
        blocks_to_be_loaded += at_least_one_passes;
    }
    for (size_t i = 0; i < block_count; ++i) {
        delete[] data[i];
    }
    delete[] data;
    return double(blocks_to_be_loaded) / double(block_count);
}

int main() {
    std::vector<int> upper_bound_range;
    for (int i = -2; i <= 46; i += 8) {
        upper_bound_range.emplace_back(i);
    }

    std::vector<size_t> block_size_range = {1 << 12, 1 << 13, 1 << 14, 1 << 15,
                                            1 << 16};

    std::ofstream out;
    //std::cout << std::filesystem::exists("blocks_to_be_loaded.csv") ;
    out.open("blocks_to_be_loaded.csv", std::ios_base::app);

    // calculate how many blocks should be loaded
    for (auto block_size : block_size_range) {
        for (auto upper_bound : upper_bound_range) {
            double to_be_loaded = blocks_to_be_loaded(block_size, upper_bound);
            double expected_ratio = (1 + to_be_loaded) / 2;
            /*std::cout << "block size: " << block_size
                      << ", upper bound: " << upper_bound
                      << ", to be loaded: " << to_be_loaded
                      << ", expected ratio: " << expected_ratio << std::endl;*/
            std::cout << block_size << "," << upper_bound << "," << to_be_loaded << "," << expected_ratio << std::endl;
        }
    }
    out.close();
}
