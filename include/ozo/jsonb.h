#pragma once

#include <ozo/type_traits.h>

#include <string>

namespace ozo::pg {

class jsonb {
public:
    jsonb() = default;

    jsonb(std::string raw_string)
        : value(std::move(raw_string)) {}

    const std::string& raw_string() const & {
        return value;
    }

    std::string& raw_string() & {
        return value;
    }

    std::string&& raw_string() && {
        return std::move(value);
    }

private:
    std::string value;
};

} // namespace ozo::pg

OZO_PG_DEFINE_TYPE_AND_ARRAY(ozo::pg::jsonb, "jsonb", JSONBOID, 3807, dynamic_size)
