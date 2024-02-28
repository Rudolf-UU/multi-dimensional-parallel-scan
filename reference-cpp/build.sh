cd "$(dirname "$0")"
mkdir -p build
clang++ -std=c++17 -stdlib=libstdc++ main.cpp -o build/main -O3 -march=native
clang++ -std=c++17 -stdlib=libstdc++ main-tbb.cpp -o build/main-tbb -O3 -march=native -I./oneTBB-install/include -L./oneTBB-install/lib -ltbb
clang++ -std=c++17 -stdlib=libstdc++ main-parlaylib.cpp -o build/main-parlaylib -O3 -march=native -pthread -I./parlaylib/include
