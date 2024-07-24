#include <vector>
#include <random>

// generate floats
class MixOfNormalDistributions {
    std::vector<std::normal_distribution<float>> distributions;
    const size_t number_of_distributions;
    size_t next_distribution;

    static std::vector<std::normal_distribution<float>> make_distributions(
                                        const std::vector<float>& means,
                                        const std::vector<float>& variances);

  public:
    MixOfNormalDistributions(const std::vector<float>& means,
                             const std::vector<float>& variances);
    std::vector<std::vector<float>> generate_blocks_float(
                                                size_t block_count,
                                                size_t block_size);
    std::vector<std::vector<int>> generate_blocks_int(size_t block_count,
                                                      size_t block_size);

};
