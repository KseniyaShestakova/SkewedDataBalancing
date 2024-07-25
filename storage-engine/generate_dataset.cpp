//#include <data_generator.h>
#include <string>
#include <data_generator_impl.h>


static const std::string kDatasetPath = "/home/xxeniash/SkewedDataBalancing/storage-engine/data/dataset";



int main() {
    std::string float_path = "/home/xxeniash/SkewedDataBalancing/storage-engine/data/dataset1_float";
    std::string int_path = "/home/xxeniash/SkewedDataBalancing/storage-engine/data/dataset1_int";

    //MixOfNormalDistributions mix(std::vector<float>(1, 1), std::vector<float>(1, 1));

    generate_dataset1<float>(float_path, 10);
    generate_dataset1<int>(int_path, 10);
}