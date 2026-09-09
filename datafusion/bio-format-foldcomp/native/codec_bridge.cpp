#include "foldcomp.h"
#include "utility.h"
#include <cmath>
#include <memory>
#include <sstream>
#include <stdexcept>

extern "C" {
struct BioAtom {
  const char *name;
  const char *residue;
  const char *chain;
  int32_t atom_id;
  int32_t residue_id;
  float x, y, z, b_factor;
};
}
struct BioFoldcomp {
  std::vector<AtomCoordinate> atoms;
  std::vector<BioAtom> rows;
  std::string title, error;
};
static void require(bool condition, const char *message) {
  if (!condition)
    throw std::runtime_error(message);
}
// Validate every size/index consumed by the upstream decoder before calling it.
static void validate(const char *data, size_t len, size_t max_atoms) {
  const uint16_t endian = 1;
  require(*reinterpret_cast<const char *>(&endian) == 1,
          "FCZ requires a little-endian target");
  require(len >= 76 && std::memcmp(data, "FCMP", 4) == 0,
          "invalid FCZ magic/header");
  CompressedFileHeader h;
  std::memcpy(&h, data + 4, sizeof h);
  require(h.nResidue >= 2 && h.nAtom >= 3 * h.nResidue && h.nAtom <= max_atoms,
          "unsupported FCZ residue/atom count");
  require(h.nAnchor >= 2 && h.nAnchor <= h.nResidue,
          "invalid FCZ anchor count");
  const size_t coordinate_start = 76 + 4 * size_t(h.nAnchor) + h.lenTitle;
  const size_t backbone_start = coordinate_start + 36 * size_t(h.nAnchor) + 13;
  const size_t expected =
      backbone_start + 9 * size_t(h.nResidue) + h.nSideChainTorsion + 8;
  require(expected == len, "FCZ length does not match header");
  int32_t previous = -1;
  for (size_t i = 0; i < h.nAnchor; ++i) {
    int32_t index;
    std::memcpy(&index, data + 76 + 4 * i, 4);
    require(index > previous && index < h.nResidue,
            "invalid FCZ anchor indices");
    if (i == 0)
      require(index == 0, "FCZ first anchor must be zero");
    if (i + 1 == h.nAnchor)
      require(index == h.nResidue - 1, "FCZ last anchor must be final residue");
    previous = index;
  }
  for (float v : h.mins)
    require(std::isfinite(v), "non-finite FCZ discretizer");
  for (float v : h.cont_fs)
    require(std::isfinite(v), "non-finite FCZ discretizer");
  for (size_t i = 0; i < 9 * size_t(h.nAnchor); ++i) {
    float v;
    std::memcpy(&v, data + coordinate_start + 4 * i, 4);
    require(std::isfinite(v), "non-finite FCZ anchor");
  }
  const size_t oxt = coordinate_start + 36 * size_t(h.nAnchor);
  require(data[oxt] == 0 || data[oxt] == 1, "invalid FCZ OXT flag");
  for (size_t i = 0; i < 3; ++i) {
    float v;
    std::memcpy(&v, data + oxt + 1 + 4 * i, 4);
    require(std::isfinite(v), "non-finite FCZ OXT");
  }
  size_t sidechains = 0;
  for (size_t i = 0; i < h.nResidue; ++i) {
    unsigned code =
        static_cast<unsigned char>(data[backbone_start + 8 * i]) >> 3;
    require(code < 20, "unsupported FCZ residue code");
    sidechains += getSideChainTorsionNum(convertIntToThreeLetterCode(code));
    if (i == 0)
      require(convertIntToOneLetterCode(code) == h.firstResidue,
              "inconsistent FCZ first residue");
    if (i + 1 == h.nResidue)
      require(convertIntToOneLetterCode(code) == h.lastResidue,
              "inconsistent FCZ last residue");
  }
  require(sidechains == h.nSideChainTorsion, "invalid FCZ sidechain count");
  for (size_t i = 0; i < 2; ++i) {
    float v;
    std::memcpy(&v,
                data + backbone_start + 8 * size_t(h.nResidue) +
                    h.nSideChainTorsion + 4 * i,
                4);
    require(std::isfinite(v), "non-finite FCZ B-factor discretizer");
  }
}
extern "C" BioFoldcomp *bio_fc_decode(const char *data, size_t len,
                                      size_t max_atoms) noexcept {
  auto result = std::unique_ptr<BioFoldcomp>(new (std::nothrow) BioFoldcomp);
  if (!result)
    return nullptr;
  try {
    validate(data, len, max_atoms);
    Foldcomp codec;
    std::istringstream input(std::string(data, len), std::ios::binary);
    require(codec.read(input) == 0 && !input.fail(), "FCZ read failed");
    require(codec.decompress(result->atoms) == 0, "FCZ decompression failed");
    require(result->atoms.size() <= max_atoms, "FCZ output exceeds max_atoms");
    result->title = codec.strTitle;
    result->rows.reserve(result->atoms.size());
    for (const auto &a : result->atoms) {
      require(std::isfinite(a.coordinate.x) && std::isfinite(a.coordinate.y) &&
                  std::isfinite(a.coordinate.z) && std::isfinite(a.tempFactor),
              "non-finite FCZ output");
      result->rows.push_back({a.atom.c_str(), a.residue.c_str(),
                              a.chain.c_str(), a.atom_index, a.residue_index,
                              a.coordinate.x, a.coordinate.y, a.coordinate.z,
                              a.tempFactor});
    }
  } catch (const std::exception &e) {
    result->error = e.what();
  } catch (...) {
    result->error = "unknown FCZ decoder failure";
  }
  return result.release();
}
extern "C" const char *bio_fc_error(const BioFoldcomp *h) noexcept {
  return h->error.c_str();
}
extern "C" const char *bio_fc_title(const BioFoldcomp *h) noexcept {
  return h->title.c_str();
}
extern "C" const BioAtom *bio_fc_atoms(const BioFoldcomp *h,
                                       size_t *n) noexcept {
  *n = h->rows.size();
  return h->rows.data();
}
extern "C" void bio_fc_free(BioFoldcomp *h) noexcept { delete h; }
