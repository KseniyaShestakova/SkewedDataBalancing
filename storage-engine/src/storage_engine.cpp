#include <storage_engine.h>
#include <string.h>
#include <cstddef>
#include <cstdlib>
#include <fstream>

void create_or_truncate(const std::string& filename) {
    int fd = open(filename.c_str(), O_RDWR | O_TRUNC | O_CREAT | O_DIRECT, 0666);
    close(fd);
}

struct alignas(512) WriteBuffer {
    char* buffer;

    WriteBuffer() {
        buffer = reinterpret_cast<char*>(aligned_alloc(512, kBlockSize));
    }

    ~WriteBuffer() {
        free(buffer);
    }

    char* get_buffer() const { return buffer; }
};

BlockMetadata::BlockMetadata() : file_id(-1), offset(-1) {}
BlockMetadata::BlockMetadata(short file_id, long offset)
    : file_id(file_id), offset(offset) {}

absl::Status BlockMetadata::sync(int fd, size_t block_id) const {
    const void* block_metadata_ptr = reinterpret_cast<const void*>(this);
    const size_t bytes_written = pwrite(fd, block_metadata_ptr, sizeof(BlockMetadata),
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

void StorageMetadata::read_existing_metadata(
    const std::filesystem::path& path) {
    // we only call this from the constructors, so it's fine to use fstream here
    std::ifstream in;
    in.open(path);

    in >> number_of_files;
    in >> block_metadata_path;

    filenames.resize(number_of_files);
    block_count_per_file.resize(number_of_files);

    for (int i = 0; i < number_of_files; ++i) {
        in >> filenames[i];
    }

    for (int i = 0; i < number_of_files; ++i) {
        in >> block_count_per_file[i];

    }
}

void StorageMetadata::create_new_storage(const std::filesystem::path& path) {
    block_metadata_path = path.generic_string() + "_block_metadata";
    number_of_files = kNumberOfFiles;
    filenames.resize(number_of_files, "");
    block_count_per_file.resize(number_of_files, 0);

    create_or_truncate(block_metadata_path);

    create_files(path);
    auto sync_res = sync(path);
    // TODO: change to PCHECK, it's a crutch
    std::cout << "Sync status: " << sync_res.ok() << '\n';
}

StorageMetadata::StorageMetadata(const std::filesystem::path& path) {
    if (std::filesystem::exists(path)) {  // create from existing file
        read_existing_metadata(path);
    } else {  // create a new metadata file
        create_new_storage(path);
    }
}

void StorageMetadata::create_files(const std::filesystem::path& path) {
    for (int i = 1; i <= number_of_files; ++i) {
        std::string filename = path.generic_string() + "_nvme" + std::to_string(i);
        create_or_truncate(filename);

        filenames[i - 1] = filename;
    }
}

absl::Status StorageMetadata::sync(const std::filesystem::path& path) const {
    create_or_truncate(path.generic_string());

    int fd = open(path.generic_string().c_str(), O_WRONLY);
    std::string output_string = std::to_string(number_of_files) + "\n";
    output_string += block_metadata_path.generic_string() + "\n";

    for (auto& filename : filenames) {
        output_string += filename + " ";
    }
    output_string += '\n';

    for (auto num : block_count_per_file) {
        output_string + std::to_string(num) + " ";
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

BlockReader::BlockReader(int fd, long offset) {
    buffer = reinterpret_cast<char*>(aligned_alloc(512, kBlockSize));
    const size_t bytes_read = pread(fd, buffer, kBlockSize, offset);
    status = (bytes_read == kBlockSize)
                 ? absl::OkStatus()
                 : absl::UnknownError(
                       "BlockReader::BlockReader error: number of read bytes is "
                       "less than expected");
}

BlockReader::BlockReader(const BlockReader& other) {
    buffer = reinterpret_cast<char*>(aligned_alloc(512, kBlockSize));
    memcpy(buffer, other.buffer, kBlockSize);
}

BlockReader::~BlockReader() {
    free(buffer);
}

bool BlockReader::is_ok() const { return status.ok(); }

absl::Status BlockReader::get_status() const { return status; }

int BlockReader::read_int(size_t num) const {
    int* int_buffer = reinterpret_cast<int*>(buffer);
    return int_buffer[num];
}

int BlockReader::read_char(size_t num) const { return buffer[num]; }

std::string
BlockReader::get_content() const {  // this function is mostly needed for testing
    std::string str(buffer);
    str.resize(kBlockSize);
    return str;
}

StorageEngine::BlockId StorageEngine::round_robin_file_selection() const {
    return next_id % storage_metadata.number_of_files;
}

StorageEngine::BlockId StorageEngine::one_disk_selection() const { return 0; }

BlockMetadata StorageEngine::get_block_metadata_from_file(size_t block_id) const{
    BlockMetadata block_metadata;
    long offset = block_id * sizeof(BlockMetadata);
    pread(block_metadata_fd, reinterpret_cast<void*>(&block_metadata),
          sizeof(block_metadata), offset);

    return block_metadata;
}

BlockMetadata StorageEngine::get_block_metadata(size_t block_id) const {
    return block_metadata_cache[block_id];
}

StorageEngine::StorageEngine(const std::filesystem::path& path)
    : path(path), next_id(0) {
    storage_metadata = StorageMetadata(path);
    next_id = storage_metadata.block_count();
    // open all files for a kind of caching
    for (int i = 0; i < kNumberOfFiles; ++i) {
        int fd = open(storage_metadata.filenames[i].c_str(), O_RDWR | O_DIRECT);
        fd_cache.emplace_back(fd);
    }
    block_metadata_fd =
        open(storage_metadata.get_block_metadata().c_str(), O_RDWR);
    auto block_count = storage_metadata.block_count();
    for (size_t block_id = 0; block_id < block_count; ++block_count) {
        block_metadata_cache.emplace_back(get_block_metadata_from_file(block_id));
    }

}

StorageEngine::~StorageEngine() {
    for (int i = 0; i < kNumberOfFiles; ++i) {
        close(fd_cache[i]);
    }
    close(block_metadata_fd);
}

absl::StatusOr<StorageEngine::BlockId> StorageEngine::create_block(
    StorageEngine::IdSelectionMode mode) {
    size_t file_id;
    switch (mode) {
    case IdSelectionMode::RoundRobin:
        file_id = round_robin_file_selection();
        break;
    case IdSelectionMode::OneDisk:
        file_id = one_disk_selection();
        break;
    default:
        file_id = round_robin_file_selection();
    }

    const size_t offset = storage_metadata.block_count_per_file[file_id] * kBlockSize;
    const BlockMetadata block_metadata = BlockMetadata(file_id, offset);

    std::filesystem::resize_file(storage_metadata.filenames[file_id],
                                 offset + kBlockSize);

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
    auto block_reader =
        BlockReader(fd, block_metadata.offset);
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

    WriteBuffer write_buffer;
    memcpy(write_buffer.get_buffer(), buffer, kBlockSize);
    const size_t bytes_written =
        pwrite(fd, write_buffer.get_buffer(), kBlockSize, block_metadata.offset);

    if (bytes_written != kBlockSize)
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

StorageMetadata StorageEngine::get_metadata() const { return this->storage_metadata; }

std::filesystem::path StorageMetadata::get_block_metadata() const{
    return this->block_metadata_path;
}

std::vector<std::string> StorageMetadata::get_filenames() const {
    return this->filenames;
}

std::vector<size_t> StorageMetadata::get_block_count_per_file() const{
    return this->block_count_per_file;
}

absl::StatusOr<int> StorageEngine::execute_query(
    const std::vector<StorageEngine::BlockId>& col_a,
    const std::vector<StorageEngine::BlockId>& col_b, int upper_bound) const {
    std::vector<bool> block_mask;
    int sum = 0;

    for (int t = 0; t < col_a.size(); ++t) {
        const BlockId col_a_block_id = col_a[t];
        const BlockId col_b_block_id = col_b[t];

        const auto get_block_a_res = get_block(col_a_block_id);
        if (!get_block_a_res.ok()) return get_block_a_res.status();
        const auto& col_a_block_reader = *get_block_a_res;
        block_mask.assign(kBlockValueCount, false);

        bool at_least_one_true = false;
        for (int i = 0; i < kBlockValueCount; ++i) {
            block_mask[i] = (col_a_block_reader.read_int(i) < upper_bound);
            at_least_one_true |= block_mask[i];
        }
        if (at_least_one_true) {
            const auto get_block_b_res = get_block(col_b_block_id);
            if (!get_block_b_res.ok()) return get_block_b_res.status();
            const auto& col_b_block_reader = *get_block_b_res;

            for (int i = 0; i < kBlockValueCount; ++i) {
                sum += block_mask[i] * col_b_block_reader.read_int(i);
            }
        }
    }
    return sum;
}

int StorageEngine::get_block_file_fd(BlockId block_id) const {
    BlockMetadata block_metadata = get_block_metadata(block_id);
    return fd_cache[block_metadata.file_id];
}
