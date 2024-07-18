#include <storage_engine.h>

void foo() {
  std::cout << "foo" << std::endl;
}



BlockMetadata::BlockMetadata() : file_id(-1),offset(-1) {}
BlockMetadata::BlockMetadata(size_t file_id, long offset) : file_id(file_id), offset(offset) {}

int BlockMetadata::sync(const std::filesystem::path& path, size_t block_id) {
    std::fstream out;
    out.open(path, std::ios::in | std::ios::out |std::ios::binary);
    if (!out.is_open()) {
        return -1;
    }
    out.seekp(sizeof(BlockMetadata) * block_id);

    char* block_metadata_ptr = reinterpret_cast<char*>(this);
    out.write(block_metadata_ptr, sizeof(BlockMetadata));

    out.close();
    return 0;
}

StorageMetadata::StorageMetadata() : block_metadata_path(),
      filenames(),
      number_of_files(NUMBER_OF_FILES) {
    filenames.reserve(number_of_files);
    block_count_per_file.resize(number_of_files, 0);
}

void StorageMetadata::read_existing_metadata(const std::filesystem::path& path) {
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

    std::ofstream out;
    out.open(block_metadata_path, std::ios::trunc);
    out.close();

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
        std::ofstream file;
        std::string filename = path.generic_string() + "_nvme" + std::to_string(i);
        file.open(filename, std::ios::trunc);
        filenames[i - 1] = filename;
        file.close();
    }
}

int StorageMetadata::sync(const std::filesystem::path& path) {
    std::ofstream out;
    out.open(path, std::ios::trunc);

    if (!out.is_open()) {
        return -1; // some error handling should be here
    }
    out.close();

    out.open(path, std::ios ::app);
    if (!out.is_open()) {
        return -1;
    }
    out << number_of_files << '\n';
    out << block_metadata_path.generic_string() << '\n';
    for (auto& filename: filenames) {
        out << filename << " ";
    }
    out << "\n";
    for (auto num: block_count_per_file) {
        out << num << " ";
    }
    out.close();
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

size_t StorageEngine::round_robin_file_selection() const {
    return next_id % storage_metadata.number_of_files;
}

size_t StorageEngine::one_disk_selection() const {
    return 0;
}

BlockMetadata StorageEngine::get_block_metadata(size_t block_id) {
    std::ifstream in;
    in.open(storage_metadata.block_metadata_path, std::ios::in | std::ios::binary);

    BlockMetadata block_metadata;
    in.seekg(block_id * sizeof(BlockMetadata));
    in.read(reinterpret_cast<char*>(&block_metadata), sizeof(block_metadata));
    in.close();
    return block_metadata;
}

StorageEngine::StorageEngine(const std::filesystem::path& path): path(path), next_id(0) {
    storage_metadata = StorageMetadata(path);
    next_id = storage_metadata.block_count();
};
size_t StorageEngine::create_block(StorageEngine::IdSelectionMode mode) {
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

auto StorageEngine::get_block(size_t block_id) {
    char* buffer = new char[BLOCK_SIZE];
    auto deleter = [](char* ptr) {
        std::cout << "called deleter\n";
        delete[] ptr;
    };
    std::unique_ptr<char, decltype(deleter)> uniq(buffer, deleter);

    BlockMetadata block_metadata = get_block_metadata(block_id);

    std::ifstream in;
    in.open(storage_metadata.filenames[block_metadata.file_id], std::ios::in);
    in.seekg(block_metadata.offset);
    in.read(buffer, BLOCK_SIZE);
    in.close();

    return uniq;

}
int StorageEngine::write(char* buffer, size_t block_id) {
    BlockMetadata block_metadata = get_block_metadata(block_id);

    std::fstream out;
    out.open(storage_metadata.filenames[block_metadata.file_id],
             std::ios::in | std::ios::out |std::ios::binary);
    if (!out.is_open()) {
        return -1;
    }
    out.seekp(block_metadata.offset);
    out.write(buffer, BLOCK_SIZE);
    out.close();

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