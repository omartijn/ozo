#pragma once

#include <ozo/detail/cancel_timer_handler.h>
#include <ozo/detail/post_handler.h>
#include <ozo/detail/timeout_handler.h>
#include <ozo/impl/io.h>
#include <ozo/impl/request_oid_map.h>
#include <ozo/time_traits.h>
#include <ozo/connection.h>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/post.hpp>

namespace ozo {
namespace impl {

template <typename ConnectionT, typename Handler>
struct connect_operation_context {
    static_assert(Connection<ConnectionT>, "ConnectionT is not a Connection");

    ConnectionT connection;
    Handler handler;

    connect_operation_context(ConnectionT connection, Handler handler)
            : connection(std::move(connection)),
              handler(std::move(handler)) {}
};

template <typename ... Ts>
using connect_operation_context_ptr = std::shared_ptr<connect_operation_context<Ts ...>>;

template <typename Connection, typename Handler>
auto make_connect_operation_context(Connection&& connection, Handler&& handler) {
    using context_type = connect_operation_context<std::decay_t<Connection>, std::decay_t<Handler>>;
    auto allocator = asio::get_associated_allocator(handler);
    return std::allocate_shared<context_type>(allocator,
        std::forward<Connection>(connection),
        std::forward<Handler>(handler)
    );
}

template <typename ... Ts>
auto& get_connection(const connect_operation_context_ptr<Ts ...>& context) noexcept {
    return context->connection;
}

template <typename ... Ts>
auto& get_handler(const connect_operation_context_ptr<Ts ...>& context) noexcept {
    return context->handler;
}

/**
* Asynchronous connection operation
*/
template <typename Context>
struct async_connect_op {
    Context context;

    template <typename TimeConstraint>
    void perform(const std::string& conninfo, const TimeConstraint& time_constrain) {
        if (error_code ec = start_connection(get_connection(context), conninfo)) {
            return done(ec);
        }

        if (connection_bad(get_connection(context))) {
            return done(error::pq_connection_status_bad);
        }

        if (error_code ec = assign_socket(get_connection(context))) {
            return done(ec);
        }

        detail::set_io_timeout(get_connection(context), get_handler(context), time_constrain);

        return write_poll(get_connection(context), *this);
    }

    void operator () (error_code ec, std::size_t = 0) {
        if (ec) {
            if (std::empty(get_error_context(get_connection(context)))) {
                set_error_context(get_connection(context), "error while connection polling");
            }
            return done(ec);
        }

        switch (connect_poll(get_connection(context))) {
            case PGRES_POLLING_OK:
                return done();

            case PGRES_POLLING_WRITING:
                return write_poll(get_connection(context), *this);

            case PGRES_POLLING_READING:
                return read_poll(get_connection(context), *this);

            case PGRES_POLLING_FAILED:
            case PGRES_POLLING_ACTIVE:
                break;
        }

        done(error::pq_connect_poll_failed);
    }

    void done(error_code ec = error_code {}) {
        std::move(get_handler(context))(std::move(ec), std::move(get_connection(context)));
    }

    using executor_type = std::decay_t<decltype(asio::get_associated_executor(get_handler(context)))>;

    executor_type get_executor() const noexcept {
        return asio::get_associated_executor(get_handler(context));
    }

    using allocator_type = std::decay_t<decltype(asio::get_associated_allocator(get_handler(context)))>;

    allocator_type get_allocator() const noexcept {
        return asio::get_associated_allocator(get_handler(context));
    }
};

template <typename Context>
inline auto make_async_connect_op(Context&& context) {
    return async_connect_op<std::decay_t<Context>> {std::forward<Context>(context)};
}

template <typename Connection, typename Handler>
inline void request_oid_map(Connection&& conn, Handler&& handler) {
    ozo::impl::request_oid_map_op op{std::forward<Handler>(handler)};
    op.perform(std::forward<Connection>(conn));
}

template <typename Handler>
struct request_oid_map_handler {
    Handler handler_;

    template <typename Connection>
    void operator() (error_code ec, Connection&& conn) {
        if (ec) {
            handler_(std::move(ec), std::forward<Connection>(conn));
        } else {
            request_oid_map(std::forward<Connection>(conn), std::move(handler_));
        }
    }

    using executor_type = decltype(asio::get_associated_executor(handler_));

    executor_type get_executor() const noexcept {
        return asio::get_associated_executor(handler_);
    }

    using allocator_type = decltype(asio::get_associated_allocator(handler_));

    allocator_type get_allocator() const noexcept {
        return asio::get_associated_allocator(handler_);
    }
};

template <typename Connection>
constexpr bool OidMapEmpty = std::is_same_v<
    std::decay_t<decltype(get_oid_map(std::declval<Connection>()))>,
    ozo::empty_oid_map
>;

template <typename Conn, typename Handler>
constexpr auto make_request_oid_map_handler(Handler&& handler, Require<!OidMapEmpty<Conn>>* = nullptr) {
    return request_oid_map_handler<std::decay_t<Handler>> {std::forward<Handler>(handler)};
}

template <typename Conn, typename Handler>
constexpr decltype(auto) make_request_oid_map_handler(Handler&& handler, Require<OidMapEmpty<Conn>>* = nullptr) {
    return std::forward<Handler>(handler);
}

template <typename C, typename TimeConstraint, typename Handler>
inline void async_connect(std::string conninfo, const TimeConstraint& time_constrain,
        C&& connection, Handler&& handler) {
    static_assert(Connection<C>, "C should model Connection concept");
    auto strand = ozo::detail::make_strand_executor(get_executor(connection));
    make_async_connect_op(
        make_connect_operation_context(
            std::forward<C>(connection),
            make_request_oid_map_handler<C>(
                asio::bind_executor(strand, detail::bind_cancel_timer<std::decay_t<TimeConstraint>>(
                    detail::post_handler(std::forward<Handler>(handler))
                ))
            )
        )
    ).perform(conninfo, time_constrain);
}

} // namespace impl
} // namespace ozo
