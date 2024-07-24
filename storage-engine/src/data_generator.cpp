#include <data_generator.h>

std::vector<std::normal_distribution<float>> MixOfNormalDistributions::make_distributions(
                            const std::vector<float>& means,
                            const std::vector<float>& variances) {
    std::vector<std::normal_distribution<float>> distributions;
    for (size_t i = 0; i < distributions.size(); ++i) {
        distributions.emplace_back(means[i], variances[i]);
    }
    return distributions;
}

MixOfNormalDistributions::MixOfNormalDistributions(const std::vector<float>& means,
                                                   const std::vector<float>& variances):
                      distributions(make_distributions(means, variances)),
                      number_of_distributions(distributions.size()),
                      next_distribution(0) {}

std::vector<std::vector<float>> MixOfNormalDistributions::generate_blocks_float(size_t block_count,
                                                                                size_t block_size) {
    std::random_device rd{};
    std::mt19937 gen{rd()};

    auto random_float = [this, &gen] {
        return distributions[next_distribution](gen);
    };

    std::vector<std::vector<float>> result;
    for (size_t i = 0; i < block_count; ++i) {
        result.emplace_back();
        for (size_t j = 0; j < block_size; ++j) {
            result.back().emplace_back(random_float());
        }
        next_distribution++;
        next_distribution %= number_of_distributions;
    }
    return result;
}

std::vector<std::vector<int>> MixOfNormalDistributions::generate_blocks_int(size_t block_count,
                                                                            size_t block_size) {
    std::random_device rd{};
    std::mt19937 gen{rd()};

    auto random_int = [this, &gen] {
        return static_cast<int>(std::round(distributions[next_distribution](gen)));
    };

    std::vector<std::vector<int>> result;
    result.reserve(block_count);
    for (size_t i = 0; i < block_count; ++i) {
        result.emplace_back();
        result.back().reserve(block_size);
        for (size_t j = 0; j < block_size; ++j) {
            result.back().emplace_back(random_int());
        }
        next_distribution++;
        next_distribution %= number_of_distributions;
    }
    return result;
}


