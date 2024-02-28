cd "$(dirname "$0")"
SCRIPT_DIR=$( pwd )

# Clean
rm -rf ./oneTBB
rm -rf ./oneTBB-install

# Clone oneTBB repository
git clone https://github.com/oneapi-src/oneTBB.git
# Use a specific commit hash for reproducability
# This is the latest commit of the master branch at the time of writing
git checkout 88f73bbb48d5384ccba3e35b5c5a59451f718063

cd oneTBB
# Create binary directory for out-of-source build
mkdir build && cd build

# Configure: customize CMAKE_INSTALL_PREFIX and disable TBB_TEST to avoid tests build
cmake -DCMAKE_INSTALL_PREFIX="${SCRIPT_DIR}/oneTBB-install" -DTBB_TEST=OFF ..
# Build
cmake --build .
# Install
cmake --install .
