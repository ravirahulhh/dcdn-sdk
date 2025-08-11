#include <iostream>
#include <random>
#include <sstream>
#include <iomanip>
#include <cstdint>

std::string generate_uuid_v4() {
    static thread_local std::mt19937_64 gen(std::random_device{}());
    static thread_local std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);

    uint64_t part1 = dist(gen);
    uint64_t part2 = dist(gen);

    part1 = (part1 & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL; // version 4
    part2 = (part2 & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL; // variant 10

    std::ostringstream oss;
    oss << std::hex << std::setfill('0')
        << std::setw(8) << (part1 >> 32)
        << "-" << std::setw(4) << ((part1 >> 16) & 0xFFFF)
        << "-" << std::setw(4) << (part1 & 0xFFFF)
        << "-" << std::setw(4) << (part2 >> 48)
        << "-" << std::setw(12) << (part2 & 0xFFFFFFFFFFFFULL);

    return oss.str();
}
