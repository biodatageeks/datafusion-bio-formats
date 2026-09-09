#include "gemmi/cif.hpp"
#include <deque>
#include <memory>

// All exposed spans live until bio_cif_free; no exception crosses the C ABI.
extern "C" {
struct BioCell {
  const char *data;
  size_t len;
};
struct BioColumn {
  BioCell name;
  const BioCell *cells;
  size_t len;
};
struct BioBlock {
  BioCell name;
  const BioColumn *columns;
  size_t len;
};
}
struct BioCif {
  std::deque<std::string> strings;
  std::deque<std::vector<BioCell>> cells;
  std::deque<std::vector<BioColumn>> columns;
  std::vector<BioBlock> blocks;
  std::string error;
  BioCell string(std::string s) {
    strings.push_back(std::move(s));
    auto &v = strings.back();
    return {v.data(), v.size()};
  }
  BioCell value(const std::string &s) {
    return gemmi::cif::is_null(s) ? BioCell{nullptr, 0}
                                  : string(gemmi::cif::as_string(s));
  }
};
extern "C" BioCif *bio_cif_read(const char *data, size_t len) noexcept {
  auto result = std::unique_ptr<BioCif>(new (std::nothrow) BioCif);
  if (!result)
    return nullptr;
  try {
    auto doc = gemmi::cif::read_memory(data, len, "mmCIF");
    for (const auto &block : doc.blocks) {
      result->columns.emplace_back();
      auto &cols = result->columns.back();
      for (const auto &item : block.items) {
        if (item.type == gemmi::cif::ItemType::Pair) {
          result->cells.push_back({result->value(item.pair[1])});
          auto &cells = result->cells.back();
          cols.push_back({result->string(gemmi::to_lower(item.pair[0])),
                          cells.data(), cells.size()});
        } else if (item.type == gemmi::cif::ItemType::Loop) {
          const auto &loop = item.loop;
          for (size_t c = 0; c < loop.width(); ++c) {
            result->cells.emplace_back();
            auto &cells = result->cells.back();
            cells.reserve(loop.length());
            for (size_t r = 0; r < loop.length(); ++r)
              cells.push_back(result->value(loop.values[r * loop.width() + c]));
            cols.push_back({result->string(gemmi::to_lower(loop.tags[c])),
                            cells.data(), cells.size()});
          }
        }
      }
      result->blocks.push_back(
          {result->string(block.name), cols.data(), cols.size()});
    }
  } catch (const std::exception &e) {
    result->error = e.what();
  } catch (...) {
    result->error = "unknown CIF parser failure";
  }
  return result.release();
}
extern "C" const char *bio_cif_error(const BioCif *h) noexcept {
  return h->error.c_str();
}
extern "C" const BioBlock *bio_cif_blocks(const BioCif *h,
                                          size_t *len) noexcept {
  *len = h->blocks.size();
  return h->blocks.data();
}
extern "C" void bio_cif_free(BioCif *h) noexcept { delete h; }
