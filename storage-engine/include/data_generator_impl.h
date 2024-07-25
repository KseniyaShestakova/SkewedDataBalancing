#include <data_generator.h>
#include <iostream>
#include <fstream>

std::vector<std::normal_distribution<float>> MixOfNormalDistributions::make_distributions(
    const std::vector<float>& means,
    const std::vector<float>& variances) {
    std::vector<std::normal_distribution<float>> distributions;
    for (size_t i = 0; i < means.size(); ++i) {
        distributions.emplace_back(means[i], variances[i]);
    }
    return distributions;
}

MixOfNormalDistributions::MixOfNormalDistributions(const std::vector<float>& means,
                                                   const std::vector<float>& variances):
      distributions(make_distributions(means, variances)),
      number_of_distributions(distributions.size()),
      next_distribution(0),
      rd(), gen(rd()){}

void MixOfNormalDistributions::change_distribution() {
    next_distribution++;
    next_distribution %= number_of_distributions;
}

template<>
float MixOfNormalDistributions::rand<float>(){
    return distributions[next_distribution](gen);
}

template<>
int MixOfNormalDistributions::rand<int>(){
    return static_cast<int>(std::round(distributions[next_distribution](gen)));
}

template <typename T>
std::vector<T> MixOfNormalDistributions::generate_vector(size_t size) {
    std::vector<T> result;
    result.reserve(size);

    for (size_t i = 0; i < size; ++i) {
        result.emplace_back(rand<T>());
        change_distribution();
    }
    return result;
}

template <typename T>
std::vector<std::vector<T>> MixOfNormalDistributions::generate_blocks(size_t block_count, size_t block_size) {
    std::vector<std::vector<T>> result;
    result.reserve(block_count);


    for (size_t i = 0; i < block_count; ++i) {
        result.emplace_back();
        result.back().reserve(block_size);
        for (size_t j = 0; j < block_size; ++j) {
            result.back().emplace_back(rand<T>());
        }
        change_distribution(); // all items in one block are from the same distribution
    }

    return result;
}

template <typename T>
void generate_dataset1(const std::string& path, int n) {
    std::vector<float> means;
    std::vector<float> variances;

    means.reserve(n);
    variances.reserve(n);

    for (int i = 0; i < n; ++i) {
        means.emplace_back(4.0 * (i + 1));
        variances.emplace_back(1);
    }

    MixOfNormalDistributions gen(means, variances);

    auto vector = gen.generate_vector<T>(100'000);
    std::ofstream out;
    out.open(path);

    for (auto item: vector) {
        out << item << " ";
        //std::cout << item << " ";
    }
    out.close();
}

template <typename T>
void foo() {
    std::cout << "foo\n";
}