#include <data_generator_impl.h>
#include <execute_query.h>
#include <storage_engine.h>

#include <cassert>
#include <chrono>
#include <cstddef>
#include <fstream>
#include <ios>
#include <string>
#include <thread>
#include <vector>


// constexpr size_t data_size = 5 * (size_t(1) << 30);
constexpr size_t data_size = size_t(2) * 1024 * 1024 * 1024;

static const std::string kStoragePath = "benchmark_store_4";

#define FLAGS_num_iterations 1

std::string mode_to_string(const StorageEngine::IdSelectionMode mode) {
    switch (mode) {
    case StorageEngine::IdSelectionMode::RoundRobin:
        return "RoundRobin";
    case StorageEngine::IdSelectionMode::OneDisk:
        return "OneDisk";
    case StorageEngine::IdSelectionMode::BatchedRoundRobin:
        return "BatchedRoundRobin";
    case StorageEngine::IdSelectionMode::Shift6:
        return "Shift6";
    default:
        return "UnrecognizedMode";
    }
}

void clean_storage(const std::filesystem::path& path) {
    std::vector<std::filesystem::path> filenames;
    for (int i = 0; i < kNumberOfFiles; ++i) {
        filenames.emplace_back(disk_pathes[i] + path.generic_string());
    }
    std::filesystem::path block_metadata =
        storage_metas_path + path.generic_string() + "_block_metadata";

    std::filesystem::remove(storage_metas_path + path.generic_string());
    for (auto& filename : filenames) {
        std::filesystem::remove(filename);
    }
    std::filesystem::remove(block_metadata);
}

void fill_storage(StorageEngine& storage_engine, size_t block_size, int n,
                  float step) {
    const size_t block_count = data_size / block_size;

    std::vector<float> means;
    std::vector<float> variances;

    means.reserve(n);
    variances.reserve(n);

    for (int i = 0; i < n; ++i) {
        means.emplace_back(step * (i + 1));
        variances.emplace_back(1);
    }

    MixOfNormalDistributions gen(means, variances);
    int** data = gen.generate_blocks<int>(block_count, block_size);

    for (size_t i = 0; i < block_count; ++i) {
        auto create_res = storage_engine.create_block();
        assert(create_res.ok() && "StorageEngine::create_block failed");
        StorageEngine::BlockId block_id = create_res.value();
        auto write_res =
            storage_engine.write(reinterpret_cast<char*>(data[i]), block_id);
    }

    for (size_t i = 0; i < block_count; ++i) {
        delete[] data[i];
    }
    delete[] data;
}

void execute_query_benchmark(StorageEngine& storage_engine, int& sum,
                             size_t block_size, int upper_bound,
                             std::vector<size_t>& cnt) {
    const size_t block_count = data_size / block_size;
    const size_t col_size = block_count / 2;  // may be it will be changed

    std::vector<StorageEngine::BlockId> col_a;
    std::vector<StorageEngine::BlockId> col_b;
    col_a.reserve(col_size);
    col_b.reserve(col_size);
    for (size_t i = 0; i < col_size; ++i) {
        col_a.emplace_back(i);
        col_b.emplace_back(i + col_size);
    }

    auto res =
        counting_execute_query(storage_engine, col_a, col_b, upper_bound, cnt);
    assert(res.ok());
    for (auto val : cnt) {
        std::cout << val << " ";
    }
    std::cout << std::endl;
}

void basic_benchmark(const std::string& log_file,
                     const StorageEngine::IdSelectionMode mode,
                     size_t batch_size = -1) {
    using namespace std::chrono_literals;

    std::ofstream out;
    out.open(log_file, std::ios_base::app | std::ios_base::out);

    std::cout << data_size << std::endl;

    std::vector<int> upper_bound_range = {0, 4, 8, 12, 20, 28};

    std::vector<size_t> block_size_range = {1 << 12, 1 << 13, 1 << 14, 1 << 15,
                                            1 << 16};

    std::vector<size_t> thread_number_range = {12};

    for (auto block_size : block_size_range) {
        std::cout << "Current block size: " << block_size << std::endl;
        std::filesystem::path path = kStoragePath;
        clean_storage(path);

        auto create_res = StorageEngine::create(path, mode, block_size, batch_size);
        assert(create_res.ok() && "StorageEngine::create failed");
        StorageEngine storage_engine = create_res.value();

        fill_storage(storage_engine, block_size, 6, 8.0);

        for (auto upper_bound : upper_bound_range) {
            for (size_t i = 0; i < FLAGS_num_iterations; ++i) {
                for (auto thread_number : thread_number_range) {
                    // make a pause between consecutive runs
                    std::this_thread::sleep_for(2000ms);

                    int execute_query_sum = 0;

                    std::cout << "data size: " << data_size
                              << ", block size: " << block_size
                              << ", upper bound: " << upper_bound
                              << ", mode: " << mode_to_string(mode) << std::endl;
                    std::vector<size_t> cnt(kNumberOfFiles);
                    execute_query_benchmark(storage_engine, execute_query_sum, block_size,
                                            upper_bound, cnt);

                    out << data_size << "," << block_size << "," << upper_bound << ","
                        << mode_to_string(mode);
                    for (auto val : cnt) {
                        out << "," << val;
                    }
                    out << std::endl;
                }
            }
        }
    }
    out.close();
}

void basic_benchmark_set(const std::string& log_file) {
    using namespace std::chrono_literals;
    basic_benchmark(log_file, StorageEngine::IdSelectionMode::RoundRobin);

    basic_benchmark(log_file, StorageEngine::IdSelectionMode::Shift6);

    basic_benchmark(log_file, StorageEngine::IdSelectionMode::OneDisk);
}

int main(int argc, char** argv) {
    basic_benchmark_set(
        "/scratch/shastako/proteus/apps/standalones/data-balancing/logs/"
        "log_retrieved_part.csv");
}

