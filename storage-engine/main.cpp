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

int main() {
    auto res = foo(-1);

    if (res.ok()) {
        std::cout << *res << std::endl;
    } else {
        auto status = res.status().message();
        std::cout << "Got error: " << status << std::endl;
        if (absl::IsInvalidArgument(res.status())) {
            std::cout << "it's invalid argument\n";
        }
        if (!absl::IsAborted(res.status())) {
            std::cout << "it's not aborted\n";
        }
    }
}
