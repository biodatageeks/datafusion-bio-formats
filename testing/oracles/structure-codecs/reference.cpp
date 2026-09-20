// Test-only driver. Compile against the archived, pinned legacy checkout;
// never link this executable into a production crate.
#include "datafusion/bio-format-structure/native/cif_bridge.cpp"
#include "datafusion/bio-format-foldcomp/native/codec_bridge.cpp"
#include <fstream>
#include <iomanip>
#include <iterator>

// Hex strings preserve raw bytes, including NULs and invalid UTF-8, without
// confusing native parser acceptance with the Rust adapter's UTF-8 checks.
static void hex(const char *data, size_t size) {
  const char digits[] = "0123456789abcdef";
  std::cout << '"';
  for (size_t i = 0; i < size; ++i) {
    const auto c = static_cast<unsigned char>(data[i]);
    std::cout << digits[c >> 4] << digits[c & 15];
  }
  std::cout << '"';
}
static void hex(const std::string &value) { hex(value.data(), value.size()); }
static void cell(const BioCell &value) {
  if (value.data)
    hex(value.data, value.len);
  else
    std::cout << "null";
}
static void cif(const std::string &input) {
  std::unique_ptr<BioCif, decltype(&bio_cif_free)> doc(
      bio_cif_read(input.data(), input.size()), bio_cif_free);
  if (!doc)
    throw std::runtime_error("CIF allocation failed");
  if (!doc->error.empty()) {
    std::cout << "{\"error_hex\":";
    hex(doc->error);
    std::cout << '}';
    return;
  }
  std::cout << "{\"blocks\":[";
  for (size_t i = 0; i < doc->blocks.size(); ++i) {
    const auto &block = doc->blocks[i];
    if (i) std::cout << ',';
    std::cout << "{\"name_hex\":";
    cell(block.name);
    std::cout << ",\"columns\":[";
    for (size_t c = 0; c < block.len; ++c) {
      const auto &column = block.columns[c];
      if (c) std::cout << ',';
      std::cout << "{\"name_hex\":";
      cell(column.name);
      std::cout << ",\"values_hex\":[";
      for (size_t r = 0; r < column.len; ++r) {
        if (r) std::cout << ',';
        cell(column.cells[r]);
      }
      std::cout << "]}";
    }
    std::cout << "]}";
  }
  std::cout << "]}";
}
template <typename T> static void integers(const T &values) {
  std::cout << '[';
  bool first = true;
  for (const auto &value : values) {
    if (!first) std::cout << ',';
    first = false;
    std::cout << +value;
  }
  std::cout << ']';
}
static uint32_t bits(float value) {
  uint32_t result;
  static_assert(sizeof(result) == sizeof(value));
  std::memcpy(&result, &value, sizeof(result));
  return result;
}
template <typename T> static void float_bits(const T &values) {
  std::vector<uint32_t> output;
  for (float value : values) output.push_back(bits(value));
  integers(output);
}
static void fcz(const std::string &input, size_t max_atoms) {
  // The adapter validates before upstream read/decompress, including malformed
  // inputs. Never call the unchecked upstream reader on arbitrary bytes.
  std::unique_ptr<BioFoldcomp, decltype(&bio_fc_free)> result(
      bio_fc_decode(input.data(), input.size(), max_atoms), bio_fc_free);
  if (!result)
    throw std::runtime_error("FCZ allocation failed");
  if (!result->error.empty()) {
    std::cout << "{\"error_hex\":";
    hex(result->error);
    std::cout << '}';
    return;
  }
  Foldcomp codec;
  std::istringstream stream(input, std::ios::binary);
  require(codec.read(stream) == 0 && !stream.fail(), "reference read failed");
  std::vector<AtomCoordinate> decoded;
  require(codec.decompress(decoded) == 0, "reference reconstruction failed");
  const auto &h = codec.header;
  std::cout << "{\"header\":[" << h.nResidue << ',' << h.nAtom << ','
            << h.idxResidue << ',' << h.idxAtom << ',' << +h.nAnchor << ','
            << +static_cast<unsigned char>(h.chain) << ',' << h.nSideChainTorsion
            << ',' << +h.firstResidue << ',' << +h.lastResidue << ',' << h.lenTitle
            << "],\"mins_bits\":";
  float_bits(h.mins);
  std::cout << ",\"factors_bits\":";
  float_bits(h.cont_fs);
  std::cout << ",\"anchors\":";
  integers(codec.anchorIndices);
  std::cout << ",\"anchor_coordinates_bits\":[";
  // Upstream stores the first anchor separately from the remaining anchors.
  for (size_t i = 0; i < codec.prevAtoms.size(); ++i) {
    if (i) std::cout << ',';
    const auto &p = codec.prevAtoms[i].coordinate;
    integers(std::vector<uint32_t>{bits(p.x), bits(p.y), bits(p.z)});
  }
  for (const auto &anchor : codec.anchorCoordinates)
    for (const auto &point : anchor) {
      std::cout << ',';
      float_bits(point);
    }
  std::cout << "],\"title_hex\":";
  hex(codec.strTitle);
  std::cout << ",\"has_oxt\":" << +codec.hasOXT << ",\"oxt_bits\":";
  integers(std::vector<uint32_t>{bits(codec.OXT_coords.x), bits(codec.OXT_coords.y),
                                  bits(codec.OXT_coords.z)});
  std::cout << ",\"backbone\":[";
  for (size_t i = 0; i < codec.compressedBackBone.size(); ++i) {
    if (i) std::cout << ',';
    const auto &b = codec.compressedBackBone[i];
    integers(std::vector<unsigned>{unsigned(b.residue), unsigned(b.phi),
        unsigned(b.psi), unsigned(b.omega), unsigned(b.n_ca_c_angle),
        unsigned(b.ca_c_n_angle), unsigned(b.c_n_ca_angle)});
  }
  std::cout << "],\"sidechain\":";
  integers(codec.sideChainAnglesDiscretized);
  std::cout << ",\"bfactor_discretizer_bits\":";
  integers(std::vector<uint32_t>{bits(codec.tempFactorsDisc.min),
                                  bits(codec.tempFactorsDisc.cont_f)});
  std::cout << ",\"bfactor_codes\":";
  integers(codec.tempFactorsDiscretized);
  std::cout << ",\"backbone_parameter_bits\":[";
  for (size_t i = 0; i < codec.compressedBackBone.size(); ++i) {
    if (i) std::cout << ',';
    const auto b = decompressBackboneChain(codec.compressedBackBone[i], h);
    float_bits(std::vector<float>{b.phi, b.psi, b.omega, b.n_ca_c_angle,
                                  b.ca_c_n_angle, b.c_n_ca_angle});
  }
  std::cout << "],\"torsion_bits\":";
  float_bits(codec.backboneTorsionAngles);
  std::cout << ",\"bond_angle_bits\":";
  float_bits(codec.backboneBondAngles);
  std::cout << ",\"sidechain_angle_bits\":[";
  for (size_t i = 0; i < codec.sideChainAnglesPerResidue.size(); ++i) {
    if (i) std::cout << ',';
    float_bits(codec.sideChainAnglesPerResidue[i]);
  }
  std::cout << ']';
  std::cout << ",\"decoded_title_hex\":";
  // Match codec.rs's CStr conversion, which truncates embedded NULs.
  hex(std::string(bio_fc_title(result.get())));
  std::cout << ",\"atoms\":[";
  for (size_t i = 0; i < result->rows.size(); ++i) {
    if (i) std::cout << ',';
    const auto &a = result->rows[i];
    std::cout << '[';
    hex(std::string(a.name)); std::cout << ',';
    hex(std::string(a.residue)); std::cout << ',';
    hex(std::string(a.chain)); std::cout << ',' << a.atom_id << ',' << a.residue_id
              << ',' << bits(a.x) << ',' << bits(a.y) << ',' << bits(a.z) << ','
              << bits(a.b_factor) << ']';
  }
  std::cout << "]}";
}
static void residue_tables() {
  std::cout << '[';
  for (unsigned code = 0; code < 32; ++code) {
    if (code) std::cout << ',';
    const auto name = convertIntToThreeLetterCode(code);
    const auto entry = Foldcomp::AAS.find(name);
    std::cout << "{\"code\":" << code << ",\"name_hex\":";
    hex(name);
    std::cout << ",\"letter\":" << +convertIntToOneLetterCode(code)
              << ",\"supported\":" << (entry != Foldcomp::AAS.end() ? "true" : "false");
    if (entry != Foldcomp::AAS.end()) {
      std::cout << ",\"sidechain_count\":" << getSideChainTorsionNum(name)
                << ",\"atom_count\":" << std::max<size_t>(3, entry->second.atoms.size());
    }
    std::cout << '}';
  }
  std::cout << ']';
}
int main(int argc, char **argv) {
  try {
    if (argc == 2 && std::string(argv[1]) == "tables") {
      residue_tables();
    } else {
      require(argc == 3 || argc == 4, "usage: reference cif|fcz input [max_atoms]");
      std::ifstream file(argv[2], std::ios::binary);
      require(file.is_open(), "cannot open input");
      const std::string input{std::istreambuf_iterator<char>(file), {}};
      if (std::string(argv[1]) == "cif") cif(input);
      else if (std::string(argv[1]) == "fcz")
        fcz(input, argc == 4 ? std::stoull(argv[3]) : 5000000);
      else throw std::runtime_error("unknown reference mode");
    }
    std::cout << '\n';
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
