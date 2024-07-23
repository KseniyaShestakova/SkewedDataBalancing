// DISCLAIMER: this file is only a playground
// it doesn't contain any useful code

#include <storage_engine.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <string.h>
#include "absl/status/statusor.h"
#include "absl/status/status.h"


absl::StatusOr<int> foo(int a) {
    if (a < 0) {
        return absl::InvalidArgumentError("negative value");
    } else {
        return a;
    }
}

struct alignas(512) WriteBuffer {
    char buffer[BLOCK_SIZE];

    char* get_buffer() {
        return buffer;
    }
};

int main() {
    std::string filename = "/home/xxeniash/SkewedDataBalancing/storage-engine/data/tmp";
    int fd = open(filename.c_str(), O_RDWR | O_TRUNC | O_CREAT | O_DIRECT, 0666);
    WriteBuffer write_buffer;
    char* buffer = write_buffer.get_buffer();
    std::cout << (size_t)&buffer % 512 << '\n';
    memset(buffer, 'a', BLOCK_SIZE);
    size_t bytes_written = pwrite(fd, buffer, BLOCK_SIZE, 0);
    std::cout << bytes_written << '\n';
    std::cout << strerror(errno) << '\n';
    memset(buffer, 'b', BLOCK_SIZE);
    std::cout << buffer << '\n';
    size_t bytes_read = pread(fd, buffer, BLOCK_SIZE, 0);
    std::cout << bytes_read << '\n';
    std::cout << strerror(errno) << '\n';
    std::cout << buffer << '\n';

}
