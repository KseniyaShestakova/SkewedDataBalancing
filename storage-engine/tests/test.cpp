#include <gtest/gtest.h>
#include <storage_engine.h>

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

TEST(StorageMetadata, NewStorage) {
    std::filesystem::path path = "new";
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

TEST(StorageEngine, NewStorage) {
    std::filesystem::path path = "new";
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
    std::filesystem::path path = "/home/shastako/new";
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
    /*std::filesystem::path path = "/home/shastako/new";
    clean_storage(path);

    StorageEngine storage_engine(path);
    auto filenames = storage_engine.get_metadata().get_filenames();
    auto block_metadata_path = storage_engine.get_metadata().get_block_metadata();

    size_t FILES_TO_CREATE = 5;

    for (int i = 0; i < FILES_TO_CREATE; ++i) {
        ASSERT_EQ(i, storage_engine.create_block(StorageEngine::IdSelectionMode::OneDisk));
        ASSERT_EQ((i + 1) * sizeof(BlockMetadata), std::filesystem::file_size(block_metadata_path));
        ASSERT_EQ((i + 1) * BLOCK_SIZE, std::filesystem::file_size(filenames[0]));
        auto block_count_per_file = storage_engine.get_metadata().get_block_count_per_file();
        ASSERT_EQ(i + 1, block_count_per_file[0]);
        for (int j = 1; j < NUMBER_OF_FILES; ++j) {
            ASSERT_EQ(0, block_count_per_file[j]);
        }
    }*/
}