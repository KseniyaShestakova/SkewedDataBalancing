#include <iostream>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <filesystem>

void foo();

static const size_t NUMBER_OF_FILES = 10; // TODO: make more flexible
static const long BLOCK_SIZE = 16;

struct BlockMetadata {
    size_t file_id;
    long offset;

    BlockMetadata();
    BlockMetadata(size_t file_id, long offset);

    int sync(const std::filesystem::path& path, size_t block_id);
};

class StorageEngine;

class StorageMetadata {
    std::filesystem::path block_metadata_path;
    std::vector<std::string> filenames;
    std::vector<size_t> block_count_per_file;
    size_t number_of_files;

    void read_existing_metadata(const std::filesystem::path& path);
    void create_new_storage(const std::filesystem::path& path);
    void create_files(const std::filesystem::path& path);
  public:
    StorageMetadata();
    explicit StorageMetadata(const std::filesystem::path& path);

    int sync(const std::filesystem::path& path);
    size_t block_count();
    std::filesystem::path get_block_metadata();
    std::vector<std::string> get_filenames();
    std::vector<size_t> get_block_count_per_file();


    friend std::ostream & operator<<( std::ostream&, const StorageMetadata&);
    friend StorageEngine;
};

class
    StorageEngine {
    std::filesystem::path path;
    size_t next_id;
    StorageMetadata storage_metadata;

    size_t round_robin_file_selection() const;
    size_t one_disk_selection() const;

    BlockMetadata get_block_metadata(size_t block_id);

  public:
    explicit StorageEngine(const std::filesystem::path& path);

    enum IdSelectionMode {
        RoundRobin,
        OneDisk,
    };
    size_t create_block(IdSelectionMode mode = IdSelectionMode::RoundRobin);

    auto get_block(size_t block_id);

    int write(char* buffer, size_t block_id);

    StorageMetadata get_metadata();

    friend std::ostream & operator<<( std::ostream&, const StorageEngine&);
};


void interpret_block_metadata_file(const std::filesystem::path& path);