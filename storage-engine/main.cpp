#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <filesystem>

static const size_t NUMBER_OF_FILES = 10; // TODO: make more flexible

struct BlockMetadata {
    size_t file_id;
    size_t offset; // offset inside a file
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
            file.open(filename);
            file << ""; // truncate if
                        // it had content
            filenames.push_back(filename);
        }
    }

    int sync(const std::filesystem::path& path) {
        std::ofstream out;
        out.open(path);

        if (!out.is_open()) {
            return -1; // some error handling should be here
        }
        out << "";
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

int create_storage_metadata(const std::filesystem::path& path) {
    std::filesystem::path block_metadata_path = path.generic_string() + "_block_metadata";

    StorageMetadata metadata = StorageMetadata(block_metadata_path, NUMBER_OF_FILES);
    metadata.create_files(path);

    // now we also want to write all of this into the file
    return metadata.sync(path);
}


class StorageEngine {
    std::filesystem::path path;
    size_t last_id;
  public:
    StorageEngine(const std::filesystem::path& path): path(path) {
        // start with creating metadata file and metadata file for blocks
          };
    size_t create_block();
    std::unique_ptr<char> get_block(size_t);
    int write(char*, size_t);
};




int main() {
    // std::cout << create_storage_metadata("/home/shastako/storage_metas/first");
    std::cout << StorageMetadata("/home/shastako/storage_metas/first");

    return 0;
}
