// DISCLAIMER: this file is only a playground
// it doesn't contain any useful code

#include <storage_engine.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <string.h>
#include "absl/status/statusor.h"
#include "absl/status/status.h"
#include <data_generator.h>

template<typename T>
auto strange_function() {
    return [] { return (T)1; };
}

int main() {
    auto f_float = strange_function<float>();
    std::cout << "trying to call f\n";
    std::cout << f_float();

    auto f_int = strange_function<int>();
    std::cout << "trying to call f\n";
    std::cout << f_int();
}
