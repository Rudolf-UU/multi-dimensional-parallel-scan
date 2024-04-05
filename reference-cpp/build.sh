cd "$(dirname "$0")"
mkdir -p build
clang++ -std=c++17 -stdlib=libstdc++ main.cpp -o build/main -O3 -march=native
