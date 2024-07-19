#include <storage_engine.h>

void foo() {
    std::cout << "foo" << std::endl;
}

void create_or_truncate(const std::string& filename) {
    int fd = open(filename.c_str(), O_RDWR | O_TRUNC | O_CREAT | O_DIRECT, 0666);
    close(fd);
}

BlockMetadata::BlockMetadata() : file_id(-1),offset(-1) {}
BlockMetadata::BlockMetadata(size_t file_id, long offset) : file_id(file_id), offset(offset) {}

int BlockMetadata::sync(const std::filesystem::path& path, size_t block_id) {
    int fd = open(path.generic_string().c_str(), O_RDWR);
    if (fd < 0) {
        return -1;
    }

    void* block_metadata_ptr = reinterpret_cast<void*>(this);
    size_t bytes_written = pwrite(fd, block_metadata_ptr, sizeof (BlockMetadata), sizeof(BlockMetadata) * block_id);

    close(fd);
    return 0;
}

StorageMetadata::StorageMetadata() : block_metadata_path(),
      filenames(),
      number_of_files(NUMBER_OF_FILES) {
    filenames.reserve(number_of_files);
    block_count_per_file.resize(number_of_files, 0);
}

void StorageMetadata::read_existing_metadata(const std::filesystem::path& path) {
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
    number_of_files = NUMBER_OF_FILES;
    filenames.resize(number_of_files, "");
    block_count_per_file.resize(number_of_files, 0);

    create_or_truncate(block_metadata_path);

    create_files(path);
    sync(path);
}


StorageMetadata::StorageMetadata(const std::filesystem::path& path) {
    if (std::filesystem::exists(path)) { // create from existing file
        read_existing_metadata(path);
    } else { // create a new metadata file
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

int StorageMetadata::sync(const std::filesystem::path& path) {
    create_or_truncate(path.generic_string());

    int fd = open(path.generic_string().c_str(), O_WRONLY);
    std::string output_string = std::to_string(number_of_files) + "\n";
    output_string += block_metadata_path.generic_string() + "\n";

    for (auto& filename: filenames) {
        output_string += filename + " ";
    }
    output_string += '\n';

    for (auto num: block_count_per_file) {
        output_string + std::to_string(num) + " ";
    }

    pwrite(fd, (void*)output_string.c_str(), output_string.size(), 0);
    close(fd);

    return 0;
}

size_t StorageMetadata::block_count() {
    size_t res = 0;
    for (int i = 0; i < number_of_files; ++i) {
        res += block_count_per_file[i];
    }
    return res;
}

std::ostream & operator<<(std::ostream& os, const StorageMetadata& metadata) {
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


BlockReader::BlockReader(const std::string& filename, size_t offset) {
    buffer = new char[BLOCK_SIZE];

    int fd = open(filename.c_str(), O_RDONLY);
    pread(fd, buffer, BLOCK_SIZE, offset);
    close(fd);
}

BlockReader::~BlockReader() {
    delete[] buffer;
}

int BlockReader::read_int(size_t num) {
    int* int_buffer = reinterpret_cast<int*>(buffer);
    return int_buffer[num];
}

int BlockReader::read_char(size_t num) {
    return buffer[num];
}

std::string BlockReader::get_content() { // this function is mostly needed for testing
    std::string str(buffer);
    str.resize(BLOCK_SIZE);
    return str;
}


StorageEngine::BlockId StorageEngine::round_robin_file_selection() const {
    return next_id % storage_metadata.number_of_files;
}

StorageEngine::BlockId StorageEngine::one_disk_selection() const {
    return 0;
}

BlockMetadata StorageEngine::get_block_metadata(size_t block_id) {
    int fd = open(storage_metadata.block_metadata_path.c_str(), O_RDONLY);

    BlockMetadata block_metadata;
    size_t offset = block_id * sizeof(BlockMetadata);
    pread(fd, reinterpret_cast<void*>(&block_metadata), sizeof(block_metadata), offset);

    close(fd);

    return block_metadata;
}

StorageEngine::StorageEngine(const std::filesystem::path& path): path(path), next_id(0) {
    storage_metadata = StorageMetadata(path);
    next_id = storage_metadata.block_count();
};

StorageEngine::BlockId StorageEngine::create_block(StorageEngine::IdSelectionMode mode) {
    size_t file_id;
    switch (mode) {
    case IdSelectionMode::RoundRobin:
        file_id = round_robin_file_selection();
        break;
    case IdSelectionMode::OneDisk:
        file_id = one_disk_selection();
    }
    size_t offset = storage_metadata.block_count_per_file[file_id] * BLOCK_SIZE;
    BlockMetadata block_metadata = BlockMetadata(file_id, offset);

    std::filesystem::resize_file(storage_metadata.filenames[file_id], offset + BLOCK_SIZE);

    int res = block_metadata.sync(storage_metadata.block_metadata_path, next_id);
    if (res == 0) {
        storage_metadata.block_count_per_file[file_id] += 1;
        storage_metadata.sync(path);
        return next_id++;
    }
    return -1;

}

BlockReader StorageEngine::get_block(StorageEngine::BlockId block_id) {
    BlockMetadata block_metadata = get_block_metadata(block_id);
    return { storage_metadata.filenames[block_metadata.file_id],
            static_cast<size_t>(block_metadata.offset) };

}
int StorageEngine::write(char* buffer, StorageEngine::BlockId block_id) {
    BlockMetadata block_metadata = get_block_metadata(block_id);

    int fd = open(storage_metadata.filenames[block_metadata.file_id].c_str(), O_WRONLY);
    pwrite(fd, buffer, BLOCK_SIZE, block_metadata.offset);
    close(fd);

    return 0;
}


std::ostream& operator<<( std::ostream& os, const StorageEngine& storage_engine) {
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
    size_t num_of_entries = std::filesystem::file_size(path) / sizeof(BlockMetadata);
    std::cout << "metadata for " << num_of_entries << " blocks:\n";
    for (size_t i = 0; i < num_of_entries; ++i) {
        BlockMetadata block_metadata;
        in.read(reinterpret_cast<char*>(&block_metadata), sizeof(block_metadata));
        std::cout << "file_id: " << block_metadata.file_id <<
            ", offset: " << block_metadata.offset << '\n';
    }
}

StorageMetadata StorageEngine::get_metadata() {
    return this->storage_metadata;
}

std::filesystem::path StorageMetadata::get_block_metadata() {
    return this->block_metadata_path;
}

std::vector<std::string> StorageMetadata::get_filenames() {
    return this->filenames;
}

std::vector<size_t > StorageMetadata::get_block_count_per_file() {
    return this->block_count_per_file;
}

int StorageEngine::execute_query(const std::vector<StorageEngine::BlockId>& col_a,
                                 const std::vector<StorageEngine::BlockId>& col_b,
                                 int upper_bound) {
    std::vector<bool> block_mask;
    int sum = 0;
    size_t BLOCK_VALUE_COUNT = BLOCK_SIZE / sizeof (int); // we assume that ints are stored in blocks

    for (int t = 0; t < col_a.size(); ++t) {
        BlockId col_a_block_id = col_a[t];
        BlockId col_b_block_id = col_b[t];

        auto col_a_block_reader = get_block(col_a_block_id);
        block_mask.assign(BLOCK_VALUE_COUNT, false);

        bool at_least_one_true = false;
        for (int i = 0; i < BLOCK_VALUE_COUNT; ++i) {
            block_mask[i] = (col_a_block_reader.read_int(i) < upper_bound);
            at_least_one_true |= block_mask[i];
        }
        if (at_least_one_true) {
            auto col_b_block_reader = get_block(col_b_block_id);

            for (int i = 0; i < BLOCK_VALUE_COUNT; ++i) {
                sum += block_mask[i] * col_b_block_reader.read_int(i);
            }
        }
    }
    return sum;
}