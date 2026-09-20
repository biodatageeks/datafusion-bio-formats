// Development-only exporter; never linked into the Rust decoder.
#include "amino_acid.h"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iostream>

static uint32_t bits(float value) {
    uint32_t result;
    std::memcpy(&result, &value, sizeof(result));
    return result;
}
int main() {
    const auto tables = AminoAcid::AminoAcids();
    for (const auto& name : {"ALA", "ARG", "ASN", "ASP", "CYS", "GLN", "GLU",
                            "GLY", "HIS", "ILE", "LEU", "LYS", "MET", "PHE",
                            "PRO", "SER", "THR", "TRP", "TYR", "VAL", "UNK"}) {
        const auto& aa = tables.at(name);
        std::cout << name << '\n';
        for (size_t i = 3; i < aa.atoms.size(); ++i) {
            const auto& atom = aa.atoms[i];
            const auto& prev = aa.sideChain.at(atom);
            std::cout << atom;
            for (const auto& p : prev) {
                const auto found = std::find(aa.atoms.begin(), aa.atoms.begin() + i, p);
                if (found == aa.atoms.begin() + i) return 1;
                std::cout << ' ' << (found - aa.atoms.begin());
            }
            std::cout << ' ' << bits(aa.bondLengths.at(prev[2] + "_" + atom))
                      << ' ' << bits(aa.bondAngles.at(prev[1] + "_" + prev[2] + "_" + atom)) << '\n';
        }
    }
}
