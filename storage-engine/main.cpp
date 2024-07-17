#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <filesystem>

static const size_t NUMBER_OF_FILES = 10; // TODO: make more flexible
static const long BLOCK_SIZE = 16;

struct BlockMetadata {
    // more fields will be added for making this more flexible
    size_t file_id;
    long offset; // offset inside a file

    BlockMetadata() : file_id(-1),offset(-1) {}
    BlockMetadata(size_t file_id, long offset) : file_id(file_id), offset(offset) {}

    int sync(const std::filesystem::path& path, size_t block_id) {
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

};

struct StorageMetadata {
    std::filesystem::path block_metadata_path;
    std::vector<std::string> filenames;
    std::vector<size_t> block_numbers;
    size_t number_of_files;

    StorageMetadata() : block_metadata_path(),
                        filenames(),
                        number_of_files(NUMBER_OF_FILES) {
        filenames.reserve(number_of_files);
        block_numbers.resize(number_of_files, 0);
    }

    StorageMetadata(const std::filesystem::path& block_metadata_path,
                    size_t number_of_files) :
          block_metadata_path(block_metadata_path),
          filenames(),
          block_numbers(),
          number_of_files(number_of_files) {
        filenames.reserve(number_of_files);
        block_numbers.resize(number_of_files, 0);
    }

    // get metadata from already existing file describing the storage
    explicit StorageMetadata(const std::filesystem::path& path) {
        std::ifstream in;
        in.open(path);

        in >> number_of_files;
        in >> block_metadata_path;

        filenames.resize(number_of_files);
        block_numbers.resize(number_of_files);

        for (int i = 0; i < number_of_files; ++i) {
            in >> filenames[i];
        }
        for (int i = 0; i < number_of_files; ++i) {
            in >> block_numbers[i];
        }
    }

    void create_files(const std::filesystem::path& path) {
        for (int i = 1; i <= number_of_files; ++i) {
            std::ofstream file;
            std::string filename = path.generic_string() + "_nvme" + std::to_string(i);
            file.open(filename, std::ios::trunc);
            filenames.push_back(filename);
        }
    }

    int sync(const std::filesystem::path& path) {
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
        for (auto num: block_numbers) {
            out << num << " ";
        }
        out.close();
        return 0;
    }

    friend std::ostream & operator<<( std::ostream&, const StorageMetadata&);
};

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
        os << metadata.block_numbers[i] << ", ";
    }
    os << "] \n";
    return os;
}

StorageMetadata create_storage(const std::filesystem::path& path) {
    std::filesystem::path block_metadata_path = path.generic_string() + "_block_metadata";
    std::ofstream out;
    out.open(block_metadata_path, std::ios::trunc);
    out.close();

    StorageMetadata metadata = StorageMetadata(block_metadata_path, NUMBER_OF_FILES);
    metadata.create_files(path);

    // now we also want to write all of this into the file
    metadata.sync(path);
    return metadata;
}


class StorageEngine {
    std::filesystem::path path;
    size_t next_id;
    StorageMetadata storage_metadata;

    size_t round_robin_file_selection() const {
        return next_id % storage_metadata.number_of_files;
    }

    size_t one_disk_selection() const {
        return 0;
    }

    BlockMetadata get_block_metadata(size_t block_id) {
        std::ifstream in;
        in.open(storage_metadata.block_metadata_path, std::ios::in | std::ios::binary);

        BlockMetadata block_metadata;
        in.seekg(block_id * sizeof(BlockMetadata));
        in.read(reinterpret_cast<char*>(&block_metadata), sizeof(block_metadata));
        in.close();
        return block_metadata;
    }

    enum IdSelectionMode {
        RoundRobin,
        OneDisk,
    };
  public:
    explicit StorageEngine(const std::filesystem::path& path): path(path), next_id(0) {
        if (std::filesystem::exists(path)) { // create from existing file
            storage_metadata = StorageMetadata(path);

            for (int i = 0; i < storage_metadata.number_of_files; ++i) {
                next_id += storage_metadata.block_numbers[i];
            }
        } else { // create a new metadata file
            storage_metadata = create_storage(path);
        }
    };
    size_t create_block(IdSelectionMode mode = IdSelectionMode::RoundRobin) {
        size_t file_id;
        switch (mode) {
        case IdSelectionMode::RoundRobin:
            file_id = round_robin_file_selection();
            break;
        case IdSelectionMode::OneDisk:
            file_id = one_disk_selection();
        }
        size_t offset = storage_metadata.block_numbers[file_id] * BLOCK_SIZE;
        BlockMetadata block_metadata = BlockMetadata(file_id, offset);

        std::filesystem::resize_file(storage_metadata.filenames[file_id], offset + BLOCK_SIZE);

        int res = block_metadata.sync(storage_metadata.block_metadata_path, next_id);
        if (res == 0) {
            storage_metadata.block_numbers[file_id] += 1;
            storage_metadata.sync(path);
            return next_id++;
        }
        return -1;

    }

    auto get_block(size_t block_id) {
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
    int write(char* buffer, size_t block_id) {
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

    friend std::ostream & operator<<( std::ostream&, const StorageEngine&);
};

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




int main() {
    //std::cout << create_storage("/home/shastako/storage_metas/second");
    //std::cout << StorageMetadata("/home/shastako/storage_metas/first");
    //std::cout << std::filesystem::exists("/home/shastako/storage_metas/first");
    //std::cout << StorageEngine("/home/shastako/storage_metas/x");
    auto storage_engine = StorageEngine("/home/shastako/storage_metas/t");
    for (int i = 0; i < 2; ++i) {
        size_t block_num = storage_engine.create_block();
        std::cout << "created block with number: " << block_num << '\n';
    }
    interpret_block_metadata_file("/home/shastako/storage_metas/t_block_metadata");
    for (int i = 0; i < BLOCK_SIZE; ++i) {
        std::cout << "a";
    }
    size_t res = storage_engine.write("xyzabcmasdfghjkl", 11);
    std::cout << res << '\n';
    auto ptr = storage_engine.get_block(11);
    std::string str(ptr.get());
    str.resize(BLOCK_SIZE);
    std::cout << "Content: " << str << '\n';
    auto second_ptr = storage_engine.get_block(0);
    str = std::string(second_ptr.get());
    str.resize(BLOCK_SIZE);
    std::cout << "Content: " << str << '\n';
    return 0;
}
