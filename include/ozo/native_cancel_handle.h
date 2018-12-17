#pragma once

#include <libpq-fe.h>
#include <memory>

namespace ozo {

struct native_cancel_handle_deleter {
    void operator() (PGcancel *ptr) const { PQfreeCancel(ptr); }
};
/**
 * libpq PGcancel safe RAII representation.
 */
using native_cancel_handle = std::unique_ptr<PGcancel, native_cancel_handle_deleter>;

using native_shared_cancel_handle = std::shared_ptr<PGcancel>;

} // namespace ozo
