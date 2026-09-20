// Standalone deterministic corruption smoke test under ASan/UBSan. No Rust
// required.
#include <cassert>
#include <fstream>
#include <iterator>
#include <random>
#include <string>
#include <vector>
extern "C" {
void *bio_fc_decode(const char *, size_t, size_t);
const char *bio_fc_error(const void *);
void bio_fc_free(void *);
}
int main(int argc, char **argv) {
  assert(argc == 2);
  std::ifstream input(argv[1], std::ios::binary);
  std::vector<char> data((std::istreambuf_iterator<char>(input)), {});
  assert(!data.empty());
  auto decode = [](const std::vector<char> &bytes, bool valid) {
    void *h = bio_fc_decode(bytes.data(), bytes.size(), 100000);
    assert(h);
    if (valid)
      assert(std::string(bio_fc_error(h)).empty());
    bio_fc_free(h);
  };
  decode(data, true);
  for (size_t n = 0; n < data.size(); ++n)
    decode(std::vector<char>(data.begin(), data.begin() + n), false);
  std::mt19937 random(455);
  for (int i = 0; i < 1000; ++i) {
    auto altered = data;
    for (int j = 0; j < 1 + i % 4; ++j)
      altered[random() % altered.size()] = char(random());
    decode(altered, false);
  }
}
