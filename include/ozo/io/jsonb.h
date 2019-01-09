#pragma once

#include <ozo/jsonb.h>
#include <ozo/io/send.h>
#include <ozo/io/recv.h>

namespace ozo {

template <>
struct size_of_impl<pg::jsonb> {
    static constexpr auto apply(const pg::jsonb& v) noexcept(noexcept(std::size(v.raw_string()))) {
        return std::size(v.raw_string()) + 1;
    }
};

template <>
struct send_impl<pg::jsonb> {
    template <typename M>
    static ostream& apply(ostream& out, const oid_map_t<M>&, const pg::jsonb& in) {
        const std::int8_t version = 1;
        write(out, version);
        return write(out, in.raw_string());
    }
};

template <>
struct recv_impl<pg::jsonb> {
    template <typename M>
    static istream& apply(istream& in, size_type size, const oid_map_t<M>&, pg::jsonb& out) {
        if (size < 1) {
            throw std::range_error("data size " + std::to_string(size) + " is too small to read jsonb");
        }
        std::int8_t version;
        read(in, version);
        out.raw_string().resize(static_cast<std::size_t>(size - 1));
        return read(in, out.raw_string());
    }
};

} // namespace ozo
