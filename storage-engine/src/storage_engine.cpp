#include <storage_engine.h>
#include <string.h>

#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <ios>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

absl::Status create_or_truncate(const std::string& filename) {
    int fd = open(filename.c_str(), O_RDWR | O_TRUNC | O_CREAT | O_DIRECT, 0666);
    if (fd < 0) {
        return absl::UnavailableError("create_or_truncate error");
    }
    close(fd);
    return absl::OkStatus();
}

struct WriteBuffer {
    const size_t block_size;
    char* buffer;

    WriteBuffer(size_t block_size) : block_size(block_size) {
        buffer = reinterpret_cast<char*>(aligned_alloc(512, block_size));
    }

    ~WriteBuffer() { free(buffer); }

    char* get_buffer() const { return buffer; }
};

BlockMetadata::BlockMetadata() : file_id(-1), offset(-1) {}
BlockMetadata::BlockMetadata(short file_id, long offset)
    : file_id(file_id), offset(offset) {}

absl::Status BlockMetadata::sync(int fd, size_t block_id) const {
    const void* block_metadata_ptr = reinterpret_cast<const void*>(this);
    const size_t bytes_written =
        pwrite(fd, block_metadata_ptr, sizeof(BlockMetadata),
               sizeof(BlockMetadata) * block_id);

    if (bytes_written == sizeof(BlockMetadata)) return absl::OkStatus();
    return absl::UnknownError(
        "BlockMetadata::sync error: number of written bytes is less than "
        "expected");
}

StorageMetadata::StorageMetadata()
    : block_metadata_path(), filenames(), number_of_files(kNumberOfFiles) {
    filenames.reserve(number_of_files);
    block_count_per_file.resize(number_of_files, 0);
}

absl::StatusOr<StorageMetadata> StorageMetadata::read_existing_metadata(
    const std::filesystem::path& path) {
    const std::string meta_path = storage_metas_path + path.generic_string();
    // we only call this from the constructors, so it's fine to use fstream here
    std::filesystem::path block_metadata_path;
    std::vector<std::string> filenames;
    std::vector<size_t> block_count_per_file;
    size_t number_of_files;

    std::ifstream in;
    in.open(meta_path);
    if (in.fail()) {
        return absl::UnavailableError(
            "StorageMetada::read_existing_metadata error: ifstream open failed");
    }

    in >> number_of_files;
    if (in.fail() || in.bad() || in.eof()) {
        return absl::UnavailableError(
            "StorageMetadata::read_existing_metadata error: reading from stream "
            "failed");
    }
    in >> block_metadata_path;
    if (in.fail() || in.bad() || in.eof()) {
        return absl::UnavailableError(
            "StorageMetadata::read_existing_metadata error: reading from stream "
            "failed");
    }

    filenames.resize(number_of_files);
    block_count_per_file.resize(number_of_files);

    for (int i = 0; i < number_of_files; ++i) {
        in >> filenames[i];
        if (in.fail() || in.bad() || in.eof()) {
            return absl::UnavailableError(
                "StorageMetadata::read_existing_metadata error: reading from stream "
                "failed");
        }
    }

    for (int i = 0; i < number_of_files; ++i) {
        in >> block_count_per_file[i];
        if (in.fail() || in.bad() || in.eof()) {
            return absl::UnavailableError(
                "StorageMetadata::read_existing_metadata error: reading from stream "
                "failed");
        }
    }
    return StorageMetadata(block_metadata_path, filenames, block_count_per_file,
                           number_of_files);
}

absl::StatusOr<StorageMetadata> StorageMetadata::create_new_storage(
    const std::filesystem::path& path) {
    std::filesystem::path block_metadata_path =
        storage_metas_path + path.generic_string() + "_block_metadata";
    size_t number_of_files = kNumberOfFiles;
    std::vector<std::string> filenames(number_of_files, "");
    std::vector<size_t> block_count_per_file(number_of_files, 0);

    auto res = create_or_truncate(block_metadata_path);
    if (!res.ok()) {
        return res;
    }

    res = create_files(path, filenames);
    if (!res.ok()) {
        return res;
    }
    StorageMetadata storage_metadata(block_metadata_path, filenames,
                                     block_count_per_file, number_of_files);
    auto sync_res = storage_metadata.sync(path);
    if (!sync_res.ok()) {
        return sync_res;
    }
    return storage_metadata;
}

StorageMetadata::StorageMetadata(
    const std::filesystem::path& block_metadata_path,
    const std::vector<std::string>& filenames,
    const std::vector<size_t> block_count_per_file, size_t number_of_files)
    : block_metadata_path(block_metadata_path),
      filenames(filenames),
      block_count_per_file(block_count_per_file),
      number_of_files(number_of_files) {}

absl::StatusOr<StorageMetadata> StorageMetadata::create(
    const std::filesystem::path& path) {
    return (std::filesystem::exists(path)) ? read_existing_metadata(path)
                                           : create_new_storage(path);
}

absl::Status StorageMetadata::create_files(
    const std::filesystem::path& path, std::vector<std::string>& filenames) {
    size_t number_of_files = filenames.size();
    for (int i = 0; i < number_of_files; ++i) {
        std::string filename = disk_pathes[i] + path.generic_string();
        auto res = create_or_truncate(filename);
        if (!res.ok()) {
            return res;
        }

        filenames[i] = filename;
    }
    return absl::OkStatus();
}

absl::Status StorageMetadata::sync(const std::filesystem::path& path) const {
    const std::string meta_path = storage_metas_path + path.generic_string();
    auto res = create_or_truncate(meta_path);
    if (!res.ok()) {
        return res;
    }

    int fd = open(meta_path.c_str(), O_WRONLY);
    std::string output_string = std::to_string(number_of_files) + "\n";
    output_string += block_metadata_path.generic_string() + "\n";

    for (auto& filename : filenames) {
        output_string += filename + " ";
    }
    output_string += '\n';

    for (auto num : block_count_per_file) {
        output_string += std::to_string(num) + " ";
    }

    size_t bytes_written =
        pwrite(fd, (void*)output_string.c_str(), output_string.size(), 0);
    close(fd);

    if (bytes_written == output_string.size()) return absl::OkStatus();
    return absl::UnknownError(
        "StorageMetadata::sync error: number of written bytes is less than "
        "expected");
}

size_t StorageMetadata::block_count() const {
    size_t res = 0;
    for (int i = 0; i < number_of_files; ++i) {
        res += block_count_per_file[i];
    }
    return res;
}

std::ostream& operator<<(std::ostream& os, const StorageMetadata& metadata) {
    os << "Number of files: " << metadata.number_of_files << '\n';
    os << "Block Metadata path: " << metadata.block_metadata_path << '\n';
    os << "Filenames: [ ";
    for (int i = 0; i < metadata.number_of_files; ++i) {
        os << metadata.filenames[i] << '\n';
    }
    os << "] \n";
    os << "Numbers of blocks: [ ";
    for (int i = 0; i < metadata.number_of_files; ++i) {
        os << metadata.block_count_per_file[i] << ", ";
    }
    os << "] \n";
    return os;
}

BlockReader::BlockReader(int fd, size_t block_size, long offset)
    : block_size(block_size) {
    buffer = reinterpret_cast<char*>(aligned_alloc(512, block_size));
    const size_t bytes_read = pread(fd, buffer, block_size, offset);
    status = (bytes_read == block_size)
                 ? absl::OkStatus()
                 : absl::UnknownError(
                       "BlockReader::BlockReader error: number of read bytes is "
                       "less than expected");
}

BlockReader::BlockReader(const BlockReader& other)
    : block_size(other.block_size) {
    buffer = reinterpret_cast<char*>(aligned_alloc(512, block_size));
    memcpy(buffer, other.buffer, block_size);
}

BlockReader& BlockReader::operator=(const BlockReader& other) {
    assert(block_size == other.block_size &&
           "can't change BlockReader block size");
    if (this != &other) {
        buffer = reinterpret_cast<char*>(aligned_alloc(512, block_size));
        memcpy(buffer, other.buffer, block_size);
    }
    return *this;
}

BlockReader::~BlockReader() { free(buffer); }

bool BlockReader::is_ok() const { return status.ok(); }

absl::Status BlockReader::get_status() const { return status; }

int BlockReader::read_int(size_t num) const {
    int* int_buffer = reinterpret_cast<int*>(buffer);
    return int_buffer[num];
}

int BlockReader::read_char(size_t num) const { return buffer[num]; }

std::string BlockReader::get_content()
    const {  // this function is mostly needed for testing
    std::string str(buffer);
    str.resize(block_size);
    return str;
}

StorageEngine::BlockId StorageEngine::round_robin_file_selection() const {
    return next_id % storage_metadata.number_of_files;
}

StorageEngine::BlockId StorageEngine::one_disk_selection() const { return 0; }

StorageEngine::BlockId StorageEngine::modulo6_selection() const {
    return (next_id / 6) % storage_metadata.number_of_files;
}

absl::StatusOr<BlockMetadata> StorageEngine::get_block_metadata_from_file(
    size_t block_id, int fd) {
    BlockMetadata block_metadata;
    long offset = block_id * sizeof(BlockMetadata);
    size_t bytes_read = pread(fd, reinterpret_cast<void*>(&block_metadata),
                              sizeof(block_metadata), offset);
    if (bytes_read < sizeof(block_metadata)) {
        return absl::UnavailableError(
            "StorageEngine::get_block_metadata_from_file error: read failed");
    }

    return block_metadata;
}

BlockMetadata StorageEngine::get_block_metadata(size_t block_id) const {
    return block_metadata_cache[block_id];
}

StorageEngine::StorageEngine(
    StorageEngine::IdSelectionMode mode, size_t block_size,
    const std::filesystem::path& path, size_t next_id,
    const StorageMetadata& storage_metadata,
    const std::vector<BlockMetadata>& block_metadata_cache,
    const std::vector<int>& fd_cache, int block_metadata_fd)
    : mode(mode),
      block_size(block_size),
      path(path),
      next_id(next_id),
      storage_metadata(storage_metadata),
      block_metadata_cache(block_metadata_cache),
      fd_cache(fd_cache),
      block_metadata_fd(block_metadata_fd) {}

StorageEngine::StorageEngine(StorageEngine::IdSelectionMode mode,
                             size_t block_size,
                             const std::filesystem::path& path, size_t next_id,
                             const StorageMetadata& storage_metadata)
    : mode(mode),
      block_size(block_size),
      path(path),
      next_id(next_id),
      storage_metadata(storage_metadata),
      block_metadata_cache(),
      fd_cache(),
      block_metadata_fd() {}

StorageEngine::StorageEngine(const StorageEngine& other)
    : StorageEngine(other.mode, other.block_size, other.path, other.next_id,
                    other.storage_metadata) {
    auto res = open_caches();
    assert(res.ok());
}

absl::Status StorageEngine::open_caches() {
    fd_cache.resize(0);
    for (int i = 0; i < kNumberOfFiles; ++i) {
        int fd = open(storage_metadata.filenames[i].c_str(), O_RDWR | O_DIRECT);
        if (fd < 0) {
            return absl::UnavailableError(
                "StorageEngine::create error: opening block file failed");
        }
        fd_cache.emplace_back(fd);
    }
    block_metadata_fd =
        open(storage_metadata.get_block_metadata().c_str(), O_RDWR);
    if (block_metadata_fd < 0) {
        return absl::UnavailableError(
            "StorageEngine::create error: opening block metadata file failed");
    }
    auto block_count = storage_metadata.block_count();
    block_metadata_cache.resize(0);
    for (size_t block_id = 0; block_id < block_count; ++block_count) {
        auto res = get_block_metadata_from_file(block_id, block_metadata_fd);
        if (!res.ok()) {
            return res.status();
        }
        block_metadata_cache.emplace_back(res.value());
    }
    return absl::OkStatus();
}

absl::StatusOr<StorageEngine> StorageEngine::create(
    const std::filesystem::path& path, StorageEngine::IdSelectionMode mode,
    size_t block_size) {
    size_t next_id = 0;
    auto create_res = StorageMetadata::create(path);
    if (!create_res.ok()) {
        return create_res.status();
    }

    StorageMetadata storage_metadata = StorageMetadata::create(path).value();
    next_id = storage_metadata.block_count();

    StorageEngine storage_engine =
        StorageEngine(mode, block_size, path, next_id, storage_metadata);
    auto res = storage_engine.open_caches();
    if (!res.ok()) return res;
    return storage_engine;
}

StorageEngine::~StorageEngine() {
    for (int i = 0; i < kNumberOfFiles; ++i) {
        close(fd_cache[i]);
    }
    close(block_metadata_fd);
}

absl::StatusOr<StorageEngine::BlockId> StorageEngine::create_block() {
    size_t file_id;
    switch (mode) {
    case IdSelectionMode::RoundRobin:
        file_id = round_robin_file_selection();
        break;
    case IdSelectionMode::OneDisk:
        file_id = one_disk_selection();
        break;
    case IdSelectionMode::Modulo6:
        file_id = modulo6_selection();
        break;
    default:
        file_id = round_robin_file_selection();
    }

    const size_t offset =
        storage_metadata.block_count_per_file[file_id] * block_size;
    const BlockMetadata block_metadata = BlockMetadata(file_id, offset);

    std::filesystem::resize_file(storage_metadata.filenames[file_id],
                                 offset + block_size);

    auto res = block_metadata.sync(block_metadata_fd, next_id);
    if (!res.ok()) return res;
    block_metadata_cache.emplace_back(block_metadata);

    storage_metadata.block_count_per_file[file_id] += 1;
    auto sync_res = storage_metadata.sync(path);
    if (!sync_res.ok()) return sync_res;
    return next_id++;
}

absl::StatusOr<BlockReader> StorageEngine::get_block(
    StorageEngine::BlockId block_id) const {
    int fd = get_block_file_fd(block_id);
    if (fd == -1) {
        return absl::UnavailableError(
            "StorageEngine::get_block error: invalid file descriptor");
    }
    const BlockMetadata block_metadata = get_block_metadata(block_id);
    auto block_reader = BlockReader(fd, block_size, block_metadata.offset);
    if (!block_reader.is_ok()) {
        return block_reader.get_status();
    }
    return block_reader;
}

absl::Status StorageEngine::write(char* buffer,
                                  StorageEngine::BlockId block_id) {
    const BlockMetadata block_metadata = get_block_metadata(block_id);

    int fd = get_block_file_fd(block_id);
    if (fd == -1)
        return absl::UnavailableError(
            "StorageEngine::write error: Invalid file descriptor");

    WriteBuffer write_buffer(block_size);
    memcpy(write_buffer.get_buffer(), buffer, block_size);
    const size_t bytes_written =
        pwrite(fd, write_buffer.get_buffer(), block_size, block_metadata.offset);

    if (bytes_written != block_size)
        return absl::UnknownError(
            "StorageEngine::write error: number of written bytes is less than "
            "expected");

    return absl::OkStatus();
}

std::ostream& operator<<(std::ostream& os,
                         const StorageEngine& storage_engine) {
    os << "Path: " << storage_engine.path << '\n';
    os << "Next id: " << storage_engine.next_id << '\n';
    os << "--------------------------------------------------------------\n";
    os << "Metadata: \n" << storage_engine.storage_metadata << '\n';
    return os;
}

void interpret_block_metadata_file(const std::filesystem::path& path) {
    std::ifstream in;
    in.open(path, std::ios::in | std::ios::binary);
    if (!in.is_open()) {
        std::cout << "error on opening the file\n";
        return;
    }
    size_t num_of_entries =
        std::filesystem::file_size(path) / sizeof(BlockMetadata);
    std::cout << "metadata for " << num_of_entries << " blocks:\n";
    for (size_t i = 0; i < num_of_entries; ++i) {
        BlockMetadata block_metadata;
        in.read(reinterpret_cast<char*>(&block_metadata), sizeof(block_metadata));
        std::cout << "file_id: " << block_metadata.file_id
                  << ", offset: " << block_metadata.offset << '\n';
    }
}

StorageMetadata StorageEngine::get_metadata() const {
    return this->storage_metadata;
}

size_t StorageEngine::get_block_size() const { return this->block_size; }

std::filesystem::path StorageMetadata::get_block_metadata() const {
    return this->block_metadata_path;
}

std::vector<std::string> StorageMetadata::get_filenames() const {
    return this->filenames;
}

std::vector<size_t> StorageMetadata::get_block_count_per_file() const {
    return this->block_count_per_file;
}

int StorageEngine::get_block_file_fd(BlockId block_id) const {
    BlockMetadata block_metadata = get_block_metadata(block_id);
    return fd_cache[block_metadata.file_id];
}
