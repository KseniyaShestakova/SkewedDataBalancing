#include <fcntl.h>
#include <unistd.h>

#include <cstddef>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

constexpr size_t kNumberOfFiles = 10;  // TODO: make more flexible
constexpr long kBlockSize = 512;
static_assert(!(kBlockSize % 512));
constexpr long kBlockValueCount = kBlockSize / sizeof(int);

class StorageEngine;

struct BlockMetadata {
    short file_id; // 2 bytes
    long offset; // type chosen so that it is consistent with offset argument of pread/pwrite

    BlockMetadata();
    BlockMetadata(short file_id, long offset);

    absl::Status sync(int fd, size_t block_id) const;
};

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

    absl::Status sync(const std::filesystem::path& path) const;
    size_t block_count() const;
    std::filesystem::path get_block_metadata() const;
    std::vector<std::string> get_filenames() const;
    std::vector<size_t> get_block_count_per_file() const;

    friend std::ostream& operator<<(std::ostream&, const StorageMetadata&);
    friend StorageEngine;
};

class BlockReader {
    char* buffer;
    absl::Status status;

  public:
    BlockReader(int fd, long offset);
    BlockReader(const BlockReader&);
    ~BlockReader();

    bool is_ok() const;
    absl::Status get_status() const;

    int read_int(size_t num) const;
    int read_char(size_t num) const;

    std::string get_content() const;
};

class StorageEngine {
  public:
    using BlockId = size_t;

  private:
    const std::filesystem::path path;
    size_t next_id;
    StorageMetadata storage_metadata;
    std::vector<BlockMetadata> block_metadata_cache;

    BlockId round_robin_file_selection() const;
    BlockId one_disk_selection() const;

    BlockMetadata get_block_metadata_from_file(size_t block_id) const;
    BlockMetadata get_block_metadata(size_t block_id) const;


    std::vector<int> fd_cache;
    int block_metadata_fd;

    int get_block_file_fd(BlockId) const;

  public:
    explicit StorageEngine(const std::filesystem::path& path);

    enum IdSelectionMode {
        RoundRobin,
        OneDisk,
    };
    absl::StatusOr<BlockId> create_block(
        IdSelectionMode mode = IdSelectionMode::RoundRobin);

    absl::StatusOr<BlockReader> get_block(BlockId block_id) const;

    absl::Status write(char* buffer, BlockId block_id);

    StorageMetadata get_metadata() const;

    absl::StatusOr<int> execute_query(const std::vector<BlockId>& col_a,
                                      const std::vector<BlockId>& col_b,
                                      int upper_bound) const;

    ~StorageEngine();

    friend std::ostream& operator<<(std::ostream&, const StorageEngine&);
};

void interpret_block_metadata_file(const std::filesystem::path& path);
