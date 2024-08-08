//#include <execute_query.h>
#include <gtest/gtest.h>
#include <gtest/internal/gtest-internal.h>
#include <storage_engine.h>

#include <cstddef>
#include <cstdlib>
#include <ctime>
//#include <platform/topology/topology.hpp>

constexpr size_t kBlockSize = 512;

static const std::string kStoragePath =
    "store";


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

void generate_strings(std::vector<std::string>& contents,
                      const size_t kBlockCount, const size_t kBlockSize) {
    contents.reserve(kBlockCount);
    char start = '0';
    char end = '~';
    char diff = end - start;
    for (size_t i = 0; i < kBlockCount; ++i) {
        std::string curr;
        for (int j = 0; j < kBlockSize; ++j) {
            curr += char(start + ((i + j) % diff));
        }
        contents.emplace_back(curr);
    }
}

void output_column(int* column, const size_t kValueCount,
                   const std::string& column_name) {
    std::cout << column_name << ": ";
    for (int i = 0; i < kValueCount; ++i) {
        std::cout << column[i] << ", ";
    }
    std::cout << '\n';
}

void check_create_block(StorageEngine& storage_engine,
                        StorageEngine::BlockId id) {
    auto res = storage_engine.create_block();
    ASSERT_EQ(res.ok(), true);
    if (res.ok()) ASSERT_EQ(*res, id);
}

/*void check_execute_query(int sum, StorageEngine& storage_engine,
                         std::vector<StorageEngine::BlockId>& col_a,
                         std::vector<StorageEngine::BlockId>& col_b,
                         int upper_bound, size_t thread_number = 1) {
    long long time = 0;
    auto res = execute_query(storage_engine, time, col_a, col_b, upper_bound,
                             thread_number);
    ASSERT_EQ(res.ok(), true);
    ASSERT_EQ(*res, sum);
}*/

/*void random_query_test(size_t blocks_per_column, const size_t block_size,
                       size_t thread_number = 1) {
    const size_t block_value_count = block_size / sizeof(int);

    std::srand(std::time(nullptr));

    std::vector<StorageEngine::BlockId> col_a;
    std::vector<StorageEngine::BlockId> col_b;
    const auto kValueCount = block_value_count * blocks_per_column;
    int* col_a_values = new int[kValueCount];
    int* col_b_values = new int[kValueCount];

    int modulo = 20;
    int upper_bound = std::rand() % modulo;
    int b_sum = 0;

    for (int i = 0; i < kValueCount; ++i) {
        col_a_values[i] = std::rand() % modulo;
        col_b_values[i] = std::rand() % modulo;

        if (col_a_values[i] < upper_bound) {
            b_sum += col_b_values[i];
        }
    }

    std::filesystem::path path = kStoragePath;
    clean_storage(path);

    auto create_res = StorageEngine::create(
        path, StorageEngine::IdSelectionMode::RoundRobin, block_size);
    ASSERT_EQ(create_res.ok(), true);
    StorageEngine storage_engine = create_res.value();

    for (int i = 0; i < blocks_per_column; ++i) {
        check_create_block(storage_engine, i);
        int* curr_block_ptr = col_a_values + i * block_value_count;
        ASSERT_EQ(
            true,
            storage_engine.write(reinterpret_cast<char*>(curr_block_ptr), i).ok());
        col_a.push_back(i);
    }

    for (int i = 0; i < blocks_per_column; ++i) {
        check_create_block(storage_engine, i + blocks_per_column);
        int* curr_block_ptr = col_b_values + i * block_value_count;
        ASSERT_EQ(true, storage_engine
                            .write(reinterpret_cast<char*>(curr_block_ptr),
                                   i + blocks_per_column)
                            .ok());
        col_b.push_back(i + blocks_per_column);
    }

    check_execute_query(b_sum, storage_engine, col_a, col_b, upper_bound,
                        thread_number);

    delete[] col_a_values;
    delete[] col_b_values;
}*/

TEST(StorageMetadata, NewStorage) {
    std::filesystem::path path = kStoragePath;
    clean_storage(path);

    auto create_res = StorageMetadata::create(path);
    ASSERT_EQ(create_res.ok(), true);
    StorageMetadata storage_metadata = create_res.value();

    std::vector<std::filesystem::path> filenames;
    for (int i = 0; i < kNumberOfFiles; ++i) {
        filenames.emplace_back(disk_pathes[i] + path.generic_string());
    }
    std::filesystem::path block_metadata =
        storage_metas_path + path.generic_string() + "_block_metadata";

    ASSERT_EQ(std::filesystem::exists(storage_metas_path + path.generic_string()),
              true);
    ASSERT_EQ(std::filesystem::exists(block_metadata), true);
    for (auto& filename : filenames) {
        ASSERT_EQ(std::filesystem::exists(filename), true);
    }
    ASSERT_EQ(0, storage_metadata.block_count());
    ASSERT_EQ(block_metadata, storage_metadata.get_block_metadata());
    for (int i = 0; i < kNumberOfFiles; ++i) {
        ASSERT_EQ(filenames[i], storage_metadata.get_filenames()[i]);
        ASSERT_EQ(0, storage_metadata.get_block_count_per_file()[i]);
    }
}

TEST(StorageMetadata, ExistingStorage) {
    std::filesystem::path path = kStoragePath;
    clean_storage(path);

    auto create_res = StorageMetadata::create(path);
    ASSERT_EQ(create_res.ok(), true);
    StorageMetadata new_storage = create_res.value();

    std::vector<std::filesystem::path> filenames;
    for (int i = 0; i < kNumberOfFiles; ++i) {
        filenames.emplace_back(disk_pathes[i] + path.generic_string());
    }
    std::filesystem::path block_metadata =
        storage_metas_path + path.generic_string() + "_block_metadata";

    create_res = StorageMetadata::create(path);
    ASSERT_EQ(create_res.ok(), true);
    StorageMetadata storage_metadata = create_res.value();

    ASSERT_EQ(0, storage_metadata.block_count());
    ASSERT_EQ(block_metadata, storage_metadata.get_block_metadata());
    for (int i = 0; i < kNumberOfFiles; ++i) {
        ASSERT_EQ(filenames[i], storage_metadata.get_filenames()[i]);
        ASSERT_EQ(0, storage_metadata.get_block_count_per_file()[i]);
    }
}

TEST(StorageEngine, NewStorage) {
    std::filesystem::path path = kStoragePath;
    clean_storage(path);
    const size_t block_size = kBlockSize;

    auto create_res = StorageEngine::create(
        path, StorageEngine::IdSelectionMode::RoundRobin, block_size);
    ASSERT_EQ(create_res.ok(), true);
    StorageEngine storage_engine = create_res.value();

    std::vector<std::filesystem::path> filenames;
    for (int i = 0; i < kNumberOfFiles; ++i) {
        filenames.emplace_back(disk_pathes[i] + path.generic_string());
    }
    std::filesystem::path block_metadata =
        storage_metas_path + path.generic_string() + "_block_metadata";

    ASSERT_EQ(std::filesystem::exists(storage_metas_path + path.generic_string()),
              true);
    ASSERT_EQ(std::filesystem::exists(block_metadata), true);
    for (auto& filename : filenames) {
        ASSERT_EQ(std::filesystem::exists(filename), true);
    }

    ASSERT_EQ(0, storage_engine.get_metadata().block_count());
    ASSERT_EQ(block_metadata, storage_engine.get_metadata().get_block_metadata());
    for (int i = 0; i < kNumberOfFiles; ++i) {
        ASSERT_EQ(filenames[i], storage_engine.get_metadata().get_filenames()[i]);
        ASSERT_EQ(0, storage_engine.get_metadata().get_block_count_per_file()[i]);
    }
}

TEST(StorageEngine, CreateBlockRoundRobin) {
    std::filesystem::path path = kStoragePath;
    clean_storage(path);
    const size_t block_size = kBlockSize;

    auto create_res = StorageEngine::create(
        path, StorageEngine::IdSelectionMode::RoundRobin, block_size);
    ASSERT_EQ(create_res.ok(), true);
    StorageEngine storage_engine = create_res.value();

    auto filenames = storage_engine.get_metadata().get_filenames();
    auto block_metadata_path = storage_engine.get_metadata().get_block_metadata();

    ASSERT_EQ(filenames.size(), kNumberOfFiles);
    for (int i = 0; i < kNumberOfFiles; ++i) {
        ASSERT_EQ(filenames[i], disk_pathes[i] + path.generic_string());
    }

    for (int i = 0; i < kNumberOfFiles; ++i) {
        check_create_block(storage_engine, i);
        ASSERT_EQ((i + 1) * sizeof(BlockMetadata),
                  std::filesystem::file_size(block_metadata_path));
        ASSERT_EQ(kBlockSize, std::filesystem::file_size(filenames[i]));
    }
    auto block_count_per_file =
        storage_engine.get_metadata().get_block_count_per_file();
    for (auto block_count : block_count_per_file) {
        ASSERT_EQ(1, block_count);
    }

    // second round
    const auto kFilesToCreate = kNumberOfFiles / 2;
    for (int i = 0; i < kFilesToCreate; ++i) {
        check_create_block(storage_engine, i + kNumberOfFiles);
        ASSERT_EQ((i + 1 + kNumberOfFiles) * sizeof(BlockMetadata),
                  std::filesystem::file_size(block_metadata_path));
        ASSERT_EQ(2 * kBlockSize, std::filesystem::file_size(filenames[i]));
    }
    block_count_per_file =
        storage_engine.get_metadata().get_block_count_per_file();
    for (int i = 0; i < kFilesToCreate; ++i) {
        ASSERT_EQ(block_count_per_file[i], 2);
    }
    for (int i = kFilesToCreate; i < kNumberOfFiles; ++i) {
        ASSERT_EQ(block_count_per_file[i], 1);
    }
}

TEST(StorageEngine, CreateBlockOneDisk) {
    std::filesystem::path path = kStoragePath;
    clean_storage(path);

    auto create_res = StorageEngine::create(
        path, StorageEngine::IdSelectionMode::OneDisk, kBlockSize);
    ASSERT_EQ(create_res.ok(), true);
    StorageEngine storage_engine = create_res.value();

    auto filenames = storage_engine.get_metadata().get_filenames();
    auto block_metadata_path = storage_engine.get_metadata().get_block_metadata();

    const size_t kBlocksToCreate = 5;

    for (int i = 0; i < kBlocksToCreate; ++i) {
        check_create_block(storage_engine, i);
        ASSERT_EQ((i + 1) * sizeof(BlockMetadata),
                  std::filesystem::file_size(block_metadata_path));
        ASSERT_EQ((i + 1) * kBlockSize, std::filesystem::file_size(filenames[0]));
        auto block_count_per_file =
            storage_engine.get_metadata().get_block_count_per_file();
        ASSERT_EQ(i + 1, block_count_per_file[0]);
        for (int j = 1; j < kNumberOfFiles; ++j) {
            ASSERT_EQ(0, block_count_per_file[j]);
        }
    }
    ASSERT_EQ(kBlocksToCreate, storage_engine.get_metadata().block_count());
}

TEST(StorageEngine, CreateBlockModulo6) {
    std::filesystem::path path = kStoragePath;
    clean_storage(path);

    auto create_res = StorageEngine::create(
        path, StorageEngine::IdSelectionMode::Modulo6, kBlockSize);
    ASSERT_EQ(create_res.ok(), true);
    StorageEngine storage_engine = create_res.value();

    auto filenames = storage_engine.get_metadata().get_filenames();
    auto block_metadata_path = storage_engine.get_metadata().get_block_metadata();

    const size_t kBlocksToCreate = 6 * kNumberOfFiles;

    for (int i = 0; i < kBlocksToCreate; ++i) {
        check_create_block(storage_engine, i);

        ASSERT_EQ((i + 1) * sizeof(BlockMetadata),
                  std::filesystem::file_size(block_metadata_path));

        size_t curr_file = (i / 6) % kNumberOfFiles;
        ASSERT_EQ(((i % 6) + 1) * kBlockSize,
                  std::filesystem::file_size(filenames[curr_file]));
        auto block_count_per_file =
            storage_engine.get_metadata().get_block_count_per_file();
        for (int j = 0; j < kNumberOfFiles; ++j) {
            if (j < curr_file) {
                ASSERT_EQ(6, block_count_per_file[j]);
            }
            if (j == curr_file) {
                ASSERT_EQ(i % 6 + 1, block_count_per_file[j]);
            }
            if (j > curr_file) {
                ASSERT_EQ(0, block_count_per_file[j]);
            }
        }
    }
    ASSERT_EQ(kBlocksToCreate, storage_engine.get_metadata().block_count());
}

TEST(StorageEngine, ReadWrite) {
    std::filesystem::path path = kStoragePath;
    clean_storage(path);

    auto create_res = StorageEngine::create(
        path, StorageEngine::IdSelectionMode::RoundRobin, kBlockSize);
    ASSERT_EQ(create_res.ok(), true);
    StorageEngine storage_engine = create_res.value();

    const size_t kBlockCount = 20;
    for (int i = 0; i < kBlockCount; ++i) {
        check_create_block(
            storage_engine,
            i);  // make this assert to ensure that eveything is created as needed
    }
    ASSERT_EQ(kBlockCount, storage_engine.get_metadata().block_count());

    std::vector<std::string> contents;
    generate_strings(contents, kBlockCount, kBlockSize);

    for (int i = 0; i < kBlockCount; ++i) {
        ASSERT_EQ(
            true,
            storage_engine.write(const_cast<char*>(contents[i].c_str()), i).ok());
    }

    for (int i = 0; i < kBlockCount; ++i) {
        auto read_res = storage_engine.get_block(i);
        ASSERT_EQ(read_res.ok(), true);
        auto str = read_res->get_content();
        ASSERT_EQ(str, contents[i]);
    }
}

TEST(StorageEngine, ReadWriteInt) {
    std::filesystem::path path = kStoragePath;
    clean_storage(path);

    auto create_res = StorageEngine::create(
        path, StorageEngine::IdSelectionMode::RoundRobin, 512);
    ASSERT_EQ(create_res.ok(), true);
    StorageEngine storage_engine = create_res.value();

    check_create_block(storage_engine, 0);

    const size_t kBlockValueCount = kBlockSize / sizeof(int);
    int* int_buffer = new int[kBlockValueCount];
    for (int i = 0; i < kBlockValueCount; ++i) {
        int_buffer[i] = i;
    }

    ASSERT_EQ(true,
              storage_engine.write(reinterpret_cast<char*>(int_buffer), 0).ok());
    auto read_res = storage_engine.get_block(0);
    ASSERT_EQ(read_res.ok(), true);
    auto block_reader = *read_res;

    for (int i = 0; i < kBlockValueCount; ++i) {
        ASSERT_EQ(int_buffer[i], block_reader.read_int(i));
    }

    delete[] int_buffer;
}

/*TEST(ExecuteQuery, OneBlockTestAllPass) {
    topology::init();
    std::filesystem::path path = kStoragePath;
    clean_storage(path);

    auto create_res = StorageEngine::create(
        path, StorageEngine::IdSelectionMode::RoundRobin, kBlockSize);
    ASSERT_EQ(create_res.ok(), true);
    StorageEngine storage_engine = create_res.value();

    auto a_create_res = storage_engine.create_block();
    auto b_create_res = storage_engine.create_block();
    ASSERT_EQ(a_create_res.ok(), true);
    ASSERT_EQ(b_create_res.ok(), true);

    auto a_block_id = *a_create_res;
    auto b_block_id = *b_create_res;

    const size_t kBlockValueCount = kBlockSize / sizeof(int);
    int* a_int_buffer = new int[kBlockValueCount];
    int* b_int_buffer = new int[kBlockValueCount];
    int b_sum = 0;
    for (int i = 0; i < kBlockValueCount; ++i) {
        a_int_buffer[i] = 0;  // any value will pass the predicate
        b_int_buffer[i] = i;
        b_sum += b_int_buffer[i];
    }

    ASSERT_EQ(true, storage_engine
                        .write(reinterpret_cast<char*>(a_int_buffer), a_block_id)
                        .ok());
    ASSERT_EQ(true, storage_engine
                        .write(reinterpret_cast<char*>(b_int_buffer), b_block_id)
                        .ok());

    std::vector<StorageEngine::BlockId> col_a(1, a_block_id);
    std::vector<StorageEngine::BlockId> col_b(1, b_block_id);

    check_execute_query(b_sum, storage_engine, col_a, col_b, 5);
}

TEST(ExecuteQuery, OneBlockTestNonePass) {
    topology::init();
    std::filesystem::path path = kStoragePath;
    clean_storage(path);

    auto create_res = StorageEngine::create(
        path, StorageEngine::IdSelectionMode::RoundRobin, kBlockSize);
    ASSERT_EQ(create_res.ok(), true);
    StorageEngine storage_engine = create_res.value();

    auto a_create_res = storage_engine.create_block();
    auto b_create_res = storage_engine.create_block();
    ASSERT_EQ(a_create_res.ok(), true);
    ASSERT_EQ(b_create_res.ok(), true);

    auto a_block_id = *a_create_res;
    auto b_block_id = *b_create_res;

    const size_t kBlockValueCount = kBlockSize / sizeof(int);
    int* a_int_buffer = new int[kBlockValueCount];
    int* b_int_buffer = new int[kBlockValueCount];
    for (int i = 0; i < kBlockValueCount; ++i) {
        a_int_buffer[i] = 6;  // any value will pass the predicate
        b_int_buffer[i] = i;
    }

    ASSERT_EQ(true, storage_engine
                        .write(reinterpret_cast<char*>(a_int_buffer), a_block_id)
                        .ok());
    ASSERT_EQ(true, storage_engine
                        .write(reinterpret_cast<char*>(b_int_buffer), b_block_id)
                        .ok());

    std::vector<StorageEngine::BlockId> col_a(1, a_block_id);
    std::vector<StorageEngine::BlockId> col_b(1, b_block_id);

    check_execute_query(0, storage_engine, col_a, col_b, 5);
}

TEST(ExecuteQuery, OneBlockTestOnePass) {
    topology::init();
    std::filesystem::path path = kStoragePath;
    clean_storage(path);

    auto create_res = StorageEngine::create(
        path, StorageEngine::IdSelectionMode::RoundRobin, kBlockSize);
    ASSERT_EQ(create_res.ok(), true);
    StorageEngine storage_engine = create_res.value();

    auto a_create_res = storage_engine.create_block();
    auto b_create_res = storage_engine.create_block();
    ASSERT_EQ(a_create_res.ok(), true);
    ASSERT_EQ(b_create_res.ok(), true);

    auto a_block_id = *a_create_res;
    auto b_block_id = *b_create_res;

    const size_t kBlockValueCount = kBlockSize / sizeof(int);
    int* a_int_buffer = new int[kBlockValueCount];
    int* b_int_buffer = new int[kBlockValueCount];
    for (int i = 0; i < kBlockValueCount; ++i) {
        a_int_buffer[i] = (i >= 2) * 6;  // any value will pass the predicate
        b_int_buffer[i] = i + 1;
    }

    ASSERT_EQ(true, storage_engine
                        .write(reinterpret_cast<char*>(a_int_buffer), a_block_id)
                        .ok());
    ASSERT_EQ(true, storage_engine
                        .write(reinterpret_cast<char*>(b_int_buffer), b_block_id)
                        .ok());

    std::vector<StorageEngine::BlockId> col_a(1, a_block_id);
    std::vector<StorageEngine::BlockId> col_b(1, b_block_id);

    check_execute_query(3, storage_engine, col_a, col_b, 5);
}

TEST(ExecuteQuery, RandomTests) {
    //topology::init();
    random_query_test(1, kBlockSize, 4);
    random_query_test(2, kBlockSize, 4);
    random_query_test(4, kBlockSize, 4);
    random_query_test(8, kBlockSize, 4);
    random_query_test(16, kBlockSize, 4);
    random_query_test(32, kBlockSize, 4);
    random_query_test(64, kBlockSize, 4);
    random_query_test(128, kBlockSize, 4);
}
*/