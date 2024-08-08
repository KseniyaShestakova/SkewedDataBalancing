#include <fcntl.h>
#include <unistd.h>

#include <cstddef>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

#pragma once

static const std::vector<std::string> disk_pathes_round_robin = {
    "/home/xxeniash/SkewedDataBalancing/storage-engine/data/nvme1/",
    "/home/xxeniash/SkewedDataBalancing/storage-engine/data/nvme2/",
    "/home/xxeniash/SkewedDataBalancing/storage-engine/data/nvme3/",
    "/home/xxeniash/SkewedDataBalancing/storage-engine/data/nvme4/",
    "/home/xxeniash/SkewedDataBalancing/storage-engine/data/nvme5/",
    "/home/xxeniash/SkewedDataBalancing/storage-engine/data/nvme6/"};

/*static const std::vector<std::string> disk_pathes_one_disk = {
    "/scratch/shastako/proteus/apps/standalones/data-balancing/data/nvme1/",
    "/scratch/shastako/proteus/apps/standalones/data-balancing/data/nvme2/",
    "/scratch/shastako/proteus/apps/standalones/data-balancing/data/nvme3/",
    "/scratch/shastako/proteus/apps/standalones/data-balancing/data/nvme4/",
    "/scratch/shastako/proteus/apps/standalones/data-balancing/data/nvme7/",
    "/scratch/shastako/proteus/apps/standalones/data-balancing/data/nvme8/",
    "/scratch/shastako/proteus/apps/standalones/data-balancing/data/nvme9/",
    "/scratch/shastako/proteus/apps/standalones/data-balancing/data/nvme10/"};*/
static const std::vector<std::string> disk_pathes_one_disk = {
    "/nvme1/shastako/nvme1", "/nvme1/shastako/nvme2", "/nvme1/shastako/nvme3",
    "/nvme1/shastako/nvme4", "/nvme1/shastako/nvme7", "/nvme1/shastako/nvme8",
    "/nvme1/shastako/nvme9", "/nvme1/shastako/nvme10"};

static const std::vector<std::string> disk_pathes = disk_pathes_round_robin;

static const size_t kNumberOfFiles = disk_pathes_round_robin.size();
static const std::string storage_metas_path =
    "/home/xxeniash/SkewedDataBalancing/storage-engine/storage_metas/";

class StorageEngine;

struct BlockMetadata {
    short file_id;
    long offset;

    BlockMetadata();
    BlockMetadata(short file_id, long offset);

    absl::Status sync(int fd, size_t block_id) const;
};

class StorageMetadata {
    std::filesystem::path block_metadata_path;
    std::vector<std::string> filenames;
    std::vector<size_t> block_count_per_file;
    size_t number_of_files;

    static absl::StatusOr<StorageMetadata> read_existing_metadata(
        const std::filesystem::path& path);
    static absl::StatusOr<StorageMetadata> create_new_storage(
        const std::filesystem::path& path);
    static absl::Status create_files(const std::filesystem::path& path,
                                     std::vector<std::string>& filenames);

    StorageMetadata(const std::filesystem::path&, const std::vector<std::string>&,
                    const std::vector<size_t>, size_t);

  public:
    StorageMetadata();
    static absl::StatusOr<StorageMetadata> create(const std::filesystem::path&);

    absl::Status sync(const std::filesystem::path& path) const;
    size_t block_count() const;
    std::filesystem::path get_block_metadata() const;
    std::vector<std::string> get_filenames() const;
    std::vector<size_t> get_block_count_per_file() const;

    friend std::ostream& operator<<(std::ostream&, const StorageMetadata&);
    friend StorageEngine;
};

class BlockReader {
    const size_t block_size;
    char* buffer;
    absl::Status status;

  public:
    BlockReader(int fd, size_t block_size, long offset);
    BlockReader(const BlockReader&);
    BlockReader& operator=(const BlockReader& other);
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
    enum IdSelectionMode { RoundRobin, OneDisk, Modulo6 };

  private:
    const IdSelectionMode mode;
    const size_t block_size;
    const std::filesystem::path path;
    size_t next_id;
    StorageMetadata storage_metadata;
    std::vector<BlockMetadata> block_metadata_cache;
    std::vector<int> fd_cache;
    int block_metadata_fd;

    BlockId round_robin_file_selection() const;
    BlockId one_disk_selection() const;
    BlockId modulo6_selection() const;

    static absl::StatusOr<BlockMetadata> get_block_metadata_from_file(
        size_t block_id, int fd);
    BlockMetadata get_block_metadata(size_t block_id) const;

    int get_block_file_fd(BlockId) const;

    StorageEngine(IdSelectionMode, size_t, const std::filesystem::path&, size_t,
                  const StorageMetadata&, const std::vector<BlockMetadata>&,
                  const std::vector<int>&, int);
    StorageEngine(IdSelectionMode, size_t, const std::filesystem::path&, size_t,
                  const StorageMetadata&);
    absl::Status open_caches();

  public:
    static absl::StatusOr<StorageEngine> create(const std::filesystem::path& path,
                                                IdSelectionMode, size_t);
    StorageEngine(const StorageEngine&);
    StorageEngine& operator=(const StorageEngine&) = delete;
    ~StorageEngine();

    absl::StatusOr<BlockId> create_block();

    absl::StatusOr<BlockReader> get_block(BlockId block_id) const;

    absl::Status write(char* buffer, BlockId block_id);

    StorageMetadata get_metadata() const;
    size_t get_block_size() const;

    friend std::ostream& operator<<(std::ostream&, const StorageEngine&);
};

void interpret_block_metadata_file(const std::filesystem::path& path);
