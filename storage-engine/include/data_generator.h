#include <vector>
#include <random>

// generate floats
class MixOfNormalDistributions {
    std::vector<std::normal_distribution<float>> distributions;
    const size_t number_of_distributions;
    size_t next_distribution;
    std::random_device rd;
    std::mt19937 gen;

    static std::vector<std::normal_distribution<float>> make_distributions(
                                        const std::vector<float>& means,
                                        const std::vector<float>& variances);
    template <typename T>
    T rand();

    void change_distribution();

    public:
    MixOfNormalDistributions(const std::vector<float>& means,
                             const std::vector<float>& variances);

    template <typename T>
    std::vector<T> generate_vector(size_t size);

    template <typename T>
    std::vector<std::vector<T>> generate_blocks(size_t block_count, size_t block_size);
};

template <typename T>
void generate_dataset1(const std::string& path, int n);

template <typename T>
void foo();


