#pragma once

#include <ozo/impl/io.h>
#include <ozo/detail/cancel_timer_handler.h>
#include <ozo/detail/post_handler.h>
#include <ozo/connection.h>
#include <ozo/type_traits.h>

#include <boost/asio/bind_executor.hpp>

namespace ozo::impl {

template <typename Connection, typename Handler>
struct cancel_op_handler {

    struct context_type {
        Connection conn;
        std::optional<Handler> handler;
        context_type(Connection conn, std::optional<Handler> handler)
        : conn(std::move(conn)), handler(std::move(handler)){}
    };

    std::shared_ptr<context_type> ctx_;

    cancel_op_handler(Connection conn, Handler handler) {
        auto allocator = asio::get_associated_allocator(handler);
        ctx_ = std::allocate_shared<context_type>(allocator, std::move(conn), std::move(handler));
    }

    void operator() (error_code ec, std::string msg) {
        if (ctx_->handler) {
            auto handler = std::move(*ctx_->handler);
            ctx_->handler = std::nullopt;
            if (ec) {
                set_error_context(ctx_->conn, std::move(msg));
                // Since there is no guarantee about cancelling operation
                // in case of error the connection is in non determined state.
                // To avoid cancelling next operation on the connection the safest
                // way is to close the connection here.
                close_connection(ctx_->conn);
            }
            std::move(handler)(std::move(ec), std::move(ctx_->conn));
        }
    }

    using executor_type = decltype(asio::get_associated_executor(*(ctx_->handler)));

    executor_type get_executor() const noexcept { return asio::get_associated_executor(*(ctx_->handler));}

    using allocator_type = decltype(asio::get_associated_allocator(*(ctx_->handler)));

    allocator_type get_allocator() const noexcept { return asio::get_associated_allocator(*(ctx_->handler));}
};

template <typename Connection, typename Handler>
struct cancel_op_timeout_handler {
    cancel_op_handler<Connection, Handler> handler_;

    cancel_op_timeout_handler(cancel_op_handler<Connection, Handler> handler)
     : handler_(std::move(handler)) {}

    void operator() (error_code ec) {
        if (ec != asio::error::operation_aborted) {
            handler_(asio::error::operation_aborted, "cancel() operation waiting aborted by time-out");
        }
    }

    using executor_type = decltype(asio::get_associated_executor(handler_));

    executor_type get_executor() const noexcept { return asio::get_associated_executor(handler_);}

    using allocator_type = decltype(asio::get_associated_allocator(handler_));

    allocator_type get_allocator() const noexcept { return asio::get_associated_allocator(handler_);}
};

template <typename Executor, typename CancelHandle, typename Connection, typename Handler>
struct cancel_op {
    Executor ex_;
    cancel_op_handler<Connection, Handler> handler_;
    CancelHandle cancel_handle_;

    cancel_op(const Executor& ex, CancelHandle cancel_handle,
        cancel_op_handler<Connection, Handler> handler)
    : ex_(ex), handler_(std::move(handler)), cancel_handle_(std::move(cancel_handle))
    {}

    void perform() {
        asio::post(std::move(*this));
    }

    using executor_type = Executor;

    executor_type get_executor() const { return ex_;}

    void operator () () {
        auto [ec, msg] = dispatch_cancel(cancel_handle_);
        asio::dispatch(detail::bind(std::move(handler_), std::move(ec), std::move(msg)));
    }
};

template <typename Conn, typename Executor, typename TimeConstraint, typename Handler>
inline void async_cancel(Conn&& conn, Executor&& ex, TimeConstraint t, Handler&& h) {
    if (auto cancel_handle = get_cancel_handle(conn)) {
        auto strand = ozo::detail::make_strand_executor(get_executor(conn));
        auto handler = cancel_op_handler {
            std::forward<Conn>(conn),
            asio::bind_executor(strand,
                detail::bind_cancel_timer<TimeConstraint>(detail::post_handler(std::move(h)))
            )
        };

        if constexpr (t != none) {
            decltype(auto) timer = get_timer(handler.ctx_->conn);
            if constexpr (std::is_same_v<TimeConstraint, time_traits::time_point>) {
                timer.expires_at(t);
            } else {
                timer.expires_after(t);
            }
            timer.async_wait(cancel_op_timeout_handler{handler});
        }

        cancel_op op{ex, std::move(cancel_handle), std::move(handler)};
        op.perform();
    } else {
        set_error_context(conn, "call failed due to probably bad state of the connection");
        asio::dispatch(detail::bind(std::move(h),
            error_code(error::pq_get_cancel_failed),
            std::forward<Conn>(conn)
        ));
    }
}

} // namespace ozo::impl
