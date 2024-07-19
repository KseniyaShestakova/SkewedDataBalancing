#include <iostream>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>

void foo();

static const size_t NUMBER_OF_FILES = 10; // TODO: make more flexible
static const long BLOCK_SIZE = 16;
static const long BLOCK_VALUE_COUNT = BLOCK_SIZE / sizeof(int);

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

class BlockReader {
    char* buffer;

  public:
    BlockReader(const std::string& filename, size_t offset);
    ~BlockReader();

    int read_int(size_t num);
    int read_char(size_t num);

    std::string get_content();
};

class
    StorageEngine {
  public:
    using BlockId = size_t;

  private:
    std::filesystem::path path;
    size_t next_id;
    StorageMetadata storage_metadata;

    BlockId round_robin_file_selection() const;
    BlockId one_disk_selection() const;

    BlockMetadata get_block_metadata(size_t block_id);

  public:

    explicit StorageEngine(const std::filesystem::path& path);

    enum IdSelectionMode {
        RoundRobin,
        OneDisk,
    };
    BlockId create_block(IdSelectionMode mode = IdSelectionMode::RoundRobin);

    BlockReader get_block(BlockId block_id);

    int write(char* buffer, BlockId block_id);

    StorageMetadata get_metadata();

    int execute_query(const std::vector<BlockId>& col_a,
                      const std::vector<BlockId>& col_b,
                      int upper_bound);

    friend std::ostream & operator<<( std::ostream&, const StorageEngine&);
};


void interpret_block_metadata_file(const std::filesystem::path& path);