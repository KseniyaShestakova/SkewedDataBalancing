#include <gtest/gtest.h>
#include <storage_engine.h>

static const std::string STORAGE_PATH = "/home/xxeniash/SkewedDataBalancing/storage-engine/storage_metas/store";

void clean_storage(const std::filesystem::path& path) {
    std::vector<std::filesystem::path> filenames;
    for (int i = 1; i <= NUMBER_OF_FILES; ++i) {
        filenames.emplace_back(path.generic_string() + "_nvme" + std::to_string(i));
    };
    std::filesystem::path block_metadata = path.generic_string() + "_block_metadata";

    std::filesystem::remove(path);
    for (auto& filename: filenames) {
        std::filesystem::remove(filename);
    }
    std::filesystem::remove(block_metadata);
}

void generate_strings(std::vector<std::string>& contents, size_t BLOCK_COUNT) {
    contents.reserve(BLOCK_COUNT);
    char start = '0';
    char end = '~';
    char diff = end - start;
    for (size_t i = 0; i < BLOCK_COUNT; ++i) {
        std::string curr;
        for (int j = 0; j < BLOCK_SIZE; ++j) {
            curr += char(start + ((i + j) % diff));
        }
        contents.emplace_back(curr);
    }
}

TEST(StorageMetadata, NewStorage) {
    std::filesystem::path path = STORAGE_PATH;
    clean_storage(path);

    StorageMetadata storage_metadata(path);

    std::vector<std::filesystem::path> filenames;
    for (int i = 1; i <= NUMBER_OF_FILES; ++i) {
        filenames.emplace_back(path.generic_string() + "_nvme" + std::to_string(i));
    }
    std::filesystem::path block_metadata = path.generic_string() + "_block_metadata";

    ASSERT_EQ(std::filesystem::exists(path), true);
    ASSERT_EQ(std::filesystem::exists(block_metadata), true);
    for (auto& filename: filenames) {
        ASSERT_EQ(std::filesystem::exists(filename), true);
    }
    ASSERT_EQ(0, storage_metadata.block_count());
    ASSERT_EQ(block_metadata, storage_metadata.get_block_metadata());
    for (int i = 0; i < NUMBER_OF_FILES; ++i) {
        ASSERT_EQ(filenames[i], storage_metadata.get_filenames()[i]);
        ASSERT_EQ(0, storage_metadata.get_block_count_per_file()[i]);
    }
}

TEST(StorageMetadata, ExistingStorage) {
    std::filesystem::path path = STORAGE_PATH;
    clean_storage(path);

    StorageMetadata new_storage(path);

    std::vector<std::filesystem::path> filenames;
    for (int i = 1; i <= NUMBER_OF_FILES; ++i) {
        filenames.emplace_back(path.generic_string() + "_nvme" + std::to_string(i));
    }
    std::filesystem::path block_metadata = path.generic_string() + "_block_metadata";

    StorageMetadata storage_metadata(path);
    ASSERT_EQ(0, storage_metadata.block_count());
    ASSERT_EQ(block_metadata, storage_metadata.get_block_metadata());
    for (int i = 0; i < NUMBER_OF_FILES; ++i) {
        ASSERT_EQ(filenames[i], storage_metadata.get_filenames()[i]);
        ASSERT_EQ(0, storage_metadata.get_block_count_per_file()[i]);
    }
}

TEST(StorageEngine, NewStorage) {
    std::filesystem::path path = STORAGE_PATH;
    clean_storage(path);

    StorageEngine storage_engine(path);

    std::vector<std::filesystem::path> filenames;
    for (int i = 1; i <= NUMBER_OF_FILES; ++i) {
        filenames.emplace_back(path.generic_string() + "_nvme" + std::to_string(i));
    }
    std::filesystem::path block_metadata = path.generic_string() + "_block_metadata";

    ASSERT_EQ(std::filesystem::exists(path), true);
    ASSERT_EQ(std::filesystem::exists(block_metadata), true);
    for (auto& filename: filenames) {
        ASSERT_EQ(std::filesystem::exists(filename), true);
    }

    ASSERT_EQ(0, storage_engine.get_metadata().block_count());
    ASSERT_EQ(block_metadata, storage_engine.get_metadata().get_block_metadata());
    for (int i = 0; i < NUMBER_OF_FILES; ++i) {
        ASSERT_EQ(filenames[i], storage_engine.get_metadata().get_filenames()[i]);
        ASSERT_EQ(0, storage_engine.get_metadata().get_block_count_per_file()[i]);
    }
}

TEST(StorageEngine, CreateBlockRoundRobin) {
    std::filesystem::path path = STORAGE_PATH;
    clean_storage(path);

    StorageEngine storage_engine(path);
    auto filenames = storage_engine.get_metadata().get_filenames();
    auto block_metadata_path = storage_engine.get_metadata().get_block_metadata();

    ASSERT_EQ(filenames.size(), NUMBER_OF_FILES);
    for (int i = 0; i < 1; ++i) {
        ASSERT_EQ(filenames[i], path.generic_string() + "_nvme" + std::to_string(i+1));
    }

    for (int i = 0; i < NUMBER_OF_FILES; ++i) {
        ASSERT_EQ(i, storage_engine.create_block(StorageEngine::IdSelectionMode::RoundRobin));
        ASSERT_EQ((i + 1) * sizeof(BlockMetadata), std::filesystem::file_size(block_metadata_path));
        ASSERT_EQ(BLOCK_SIZE, std::filesystem::file_size(filenames[i]));
    }
    auto block_count_per_file = storage_engine.get_metadata().get_block_count_per_file();
    for (auto block_count: block_count_per_file) {
        ASSERT_EQ(1, block_count);
    }

    // second round
    auto FILES_TO_CREATE = NUMBER_OF_FILES / 2;
    for (int i = 0; i < FILES_TO_CREATE; ++i) {
        ASSERT_EQ(i + NUMBER_OF_FILES, storage_engine.create_block(StorageEngine::IdSelectionMode::RoundRobin));
        ASSERT_EQ((i + 1 + NUMBER_OF_FILES) * sizeof(BlockMetadata), std::filesystem::file_size(block_metadata_path));
        ASSERT_EQ(2 * BLOCK_SIZE, std::filesystem::file_size(filenames[i]));
    }
    block_count_per_file = storage_engine.get_metadata().get_block_count_per_file();
    for (int i = 0; i < FILES_TO_CREATE; ++i) {
        ASSERT_EQ(block_count_per_file[i], 2);
    }
    for (int i = FILES_TO_CREATE; i < NUMBER_OF_FILES; ++i) {
        ASSERT_EQ(block_count_per_file[i], 1);
    }
}

TEST(StorageEngine, CreateBlockOneDisk) {
    std::filesystem::path path = STORAGE_PATH;
    clean_storage(path);

    StorageEngine storage_engine(path);
    auto filenames = storage_engine.get_metadata().get_filenames();
    auto block_metadata_path = storage_engine.get_metadata().get_block_metadata();

    size_t BLOCKS_TO_CREATE = 5;

    for (int i = 0; i < BLOCKS_TO_CREATE; ++i) {
        ASSERT_EQ(i, storage_engine.create_block(StorageEngine::IdSelectionMode::OneDisk));
        ASSERT_EQ((i + 1) * sizeof(BlockMetadata), std::filesystem::file_size(block_metadata_path));
        ASSERT_EQ((i + 1) * BLOCK_SIZE, std::filesystem::file_size(filenames[0]));
        auto block_count_per_file = storage_engine.get_metadata().get_block_count_per_file();
        ASSERT_EQ(i + 1, block_count_per_file[0]);
        for (int j = 1; j < NUMBER_OF_FILES; ++j) {
            ASSERT_EQ(0, block_count_per_file[j]);
        }
    }
    ASSERT_EQ(BLOCKS_TO_CREATE, storage_engine.get_metadata().block_count());
}

TEST(StorageEngine, ReadWrite) {
    std::filesystem::path path = STORAGE_PATH;
    clean_storage(path);

    StorageEngine storage_engine(path);

    size_t BLOCK_COUNT = 20;
    for (int i = 0; i < BLOCK_COUNT; ++i) {
        ASSERT_EQ(i, storage_engine.create_block()); // make this assert to ensure that eveything is created as needed
    }
    ASSERT_EQ(BLOCK_COUNT, storage_engine.get_metadata().block_count());

    std::vector<std::string> contents;
    generate_strings(contents, BLOCK_COUNT);

    for (int i = 0; i < BLOCK_COUNT; ++i) {
        ASSERT_EQ(0, storage_engine.write(const_cast<char*>(contents[i].c_str()), i));
    }

    for (int i = 0; i < BLOCK_COUNT; ++i) {
        auto block_reader = storage_engine.get_block(i);
        auto str = block_reader.get_content();
        ASSERT_EQ(str, contents[i]);
    }
}

TEST(StorageEngine, ReadWriteInt) {
    std::filesystem::path path = STORAGE_PATH;
    clean_storage(path);

    StorageEngine storage_engine(path);

    ASSERT_EQ(0, storage_engine.create_block());

    int* int_buffer = new int[BLOCK_VALUE_COUNT];
    for (int i = 0; i < BLOCK_VALUE_COUNT; ++i) {
        int_buffer[i] = i;
    }

    ASSERT_EQ(0, storage_engine.write(reinterpret_cast<char*>(int_buffer), 0));
    auto block_reader = storage_engine.get_block(0);

    for (int i = 0; i < BLOCK_VALUE_COUNT; ++i) {
        ASSERT_EQ(int_buffer[i], block_reader.read_int(i));
    }

    delete[] int_buffer;
}

TEST(ExecuteQuery, OneBlockTestAllPass) {
    std::filesystem::path path = STORAGE_PATH;
    clean_storage(path);

    StorageEngine storage_engine(path);

    auto a_block_id = storage_engine.create_block();
    auto b_block_id = storage_engine.create_block();

    int* a_int_buffer = new int[BLOCK_VALUE_COUNT];
    int* b_int_buffer = new int[BLOCK_VALUE_COUNT];
    int b_sum = 0;
    for (int i = 0; i < BLOCK_VALUE_COUNT; ++i) {
        a_int_buffer[i] = 0; // any value will pass the predicate
        b_int_buffer[i] = i;
        b_sum += b_int_buffer[i];
    }

    ASSERT_EQ(0, storage_engine.write(reinterpret_cast<char*>(a_int_buffer), a_block_id));
    ASSERT_EQ(0, storage_engine.write(reinterpret_cast<char*>(b_int_buffer), b_block_id));

    std::vector<StorageEngine::BlockId> col_a(1, a_block_id);
    std::vector<StorageEngine::BlockId> col_b(1, b_block_id);

    ASSERT_EQ(b_sum, storage_engine.execute_query(col_a, col_b, 5));
}

TEST(ExecuteQuery, OneBlockTestNonePass) {
    std::filesystem::path path = STORAGE_PATH;
    clean_storage(path);

    StorageEngine storage_engine(path);

    auto a_block_id = storage_engine.create_block();
    auto b_block_id = storage_engine.create_block();

    int* a_int_buffer = new int[BLOCK_VALUE_COUNT];
    int* b_int_buffer = new int[BLOCK_VALUE_COUNT];
    for (int i = 0; i < BLOCK_VALUE_COUNT; ++i) {
        a_int_buffer[i] = 6; // any value will pass the predicate
        b_int_buffer[i] = i;
    }

    ASSERT_EQ(0, storage_engine.write(reinterpret_cast<char*>(a_int_buffer), a_block_id));
    ASSERT_EQ(0, storage_engine.write(reinterpret_cast<char*>(b_int_buffer), b_block_id));

    std::vector<StorageEngine::BlockId> col_a(1, a_block_id);
    std::vector<StorageEngine::BlockId> col_b(1, b_block_id);

    ASSERT_EQ(0, storage_engine.execute_query(col_a, col_b, 5));
}

TEST(ExecuteQuery, OneBlockTestHalfPass) {
    std::filesystem::path path = STORAGE_PATH;
    clean_storage(path);

    StorageEngine storage_engine(path);

    auto a_block_id = storage_engine.create_block();
    auto b_block_id = storage_engine.create_block();

    int* a_int_buffer = new int[BLOCK_VALUE_COUNT];
    int* b_int_buffer = new int[BLOCK_VALUE_COUNT];
    for (int i = 0; i < BLOCK_VALUE_COUNT; ++i) {
        a_int_buffer[i] = (i >= 2) * 6; // any value will pass the predicate
        b_int_buffer[i] = i + 1;
    }

    ASSERT_EQ(0, storage_engine.write(reinterpret_cast<char*>(a_int_buffer), a_block_id));
    ASSERT_EQ(0, storage_engine.write(reinterpret_cast<char*>(b_int_buffer), b_block_id));

    std::vector<StorageEngine::BlockId> col_a(1, a_block_id);
    std::vector<StorageEngine::BlockId> col_b(1, b_block_id);

    ASSERT_EQ(3, storage_engine.execute_query(col_a, col_b, 5));
}