#pragma once

#include <ozo/type_traits.h>
#include <ozo/asio.h>
#include <ozo/deadline.h>
#include <ozo/connection.h>

#include <tuple>


/**
 * @defgroup group-failover Failover
 * @brief Failover microframwork for database-related operations
 */

/**
 * @defgroup group-failover-strategy Strategy interface
 * @ingroup group-failover
 * @brief Failover microframwork strategy extention interface
 */

namespace ozo::failover {

namespace hana = boost::hana;

/**
 * @brief Basic operation context
 *
 * @tparam ConnectionProvider --- #ConnectionProvider type.
 * @tparam TimeConstraint --- operation #TimeConstraint type.
 * @tparam Args --- other operation arguments' type.
 * @ingroup group-failover-strategy
 */
template <
    typename ConnectionProvider,
    typename TimeConstraint,
    typename ...Args>
struct basic_context {
    static_assert(ozo::TimeConstraint<TimeConstraint>, "TimeConstraint should models ozo::TimeConstraint");
    static_assert(ozo::ConnectionProvider<ConnectionProvider>, "ConnectionProvider should models ozo::ConnectionProvider");

    std::decay_t<ConnectionProvider> provider; //!< #ConnectionProvider for an operation, typically deduced from operation's 1st argument.
    TimeConstraint time_constraint; //!< #TimeConstraint for an operation, typically deduced from operation's 2nd argument.
    hana::tuple<std::decay_t<Args>...> args; //!< Other arguments of an operation except #CompletionToken.

    /**
     * @brief Construct a new basic context object
     *
     * @param p --- #ConnectionProvider for an operation.
     * @param t --- #TimeConstraint for an operation.
     * @param args --- other arguments of an operation except #CompletionToken.
     */
    basic_context(ConnectionProvider p, TimeConstraint t, Args ...args)
    : provider(std::forward<ConnectionProvider>(p)),
      time_constraint(t),
      args(std::forward<Args>(args)...) {}
};

template <typename Strategy, typename Operation>
struct get_first_try_impl {
    template <typename Allocator, typename ...Args>
    static auto apply(const Operation& op, const Strategy& s, const Allocator& alloc, Args&& ...args) {
        return s.get_first_try(op, alloc, std::forward<Args>(args)...);
    }
};

/**
 * @brief Get the first try object for an operation
 *
 * This function is a part of failover strategy interface. It creates the first try of operation
 * execution context. The context data should be allocated via the specified allocator. This function
 * would be called once during a failover operation execution. By default #FailoverStrategy should
 * have `get_first_try(const Operation& op, const Allocator& alloc, Args&& ...args) const` member function.
 *
 * @param op --- operation for which the try object should be created.
 * @param strategy --- strategy according to which the try object should be created.
 * @param alloc --- allocator for try object.
 * @param args --- operation arguments except #CompletionToken.
 * @return #FailoverTry object.
 *
 * ###Customization Point
 *
 * This function may be customized for a #FailoverStrategy via specialization
 * of `ozo::failover::get_first_try_impl`. E.g.:
 * @code
namespace ozo::failover {

template <typename Operation>
struct get_first_try_impl<my_strategy, Operation> {
    template <typename Allocator, typename ...Args>
    static auto apply(const Operation&, const my_strategy& s, const Allocator& alloc, Args&& ...args) {
        return s.get_start_context<Operation>(alloc, std::forward<Args>(args)...);
    }
};

} // namespace ozo::failover
 * @endcode
 * @ingroup group-failover-strategy
 */
template <typename Strategy, typename Operation, typename Allocator, typename ...Args>
inline auto get_first_try(const Operation& op, const Strategy& strategy, const Allocator& alloc, Args&& ...args) {
    return get_first_try_impl<Strategy, Operation>::apply(op, strategy, alloc, std::forward<Args>(args)...);
}

template <typename Try>
struct get_try_context_impl {
    static decltype(auto) apply(const Try& a_try) {
        return a_try.get_context();
    }
};

/**
 * @brief Get operation context for the try
 *
 * By default #FailoverTry object should have `get_context() const` member function.
 *
 * @param a_try --- #FailoverTry object
 * @return auto --- #HanaSequence with operation arguments.
 *
 * ###Customization Point
 *
 * This function may be customized for a #FailoverTry via specialization
 * of `ozo::failover::get_try_context_impl`. E.g.:
 * @code
namespace ozo::failover {

template <>
struct get_try_context_impl<my_strategy_try> {
    template <typename Allocator, typename ...Args>
    static auto apply(const my_strategy_try& obj) {
        return obj.ctx;
    }
};

} // namespace ozo::failover
 * @endcode
 * @ingroup group-failover-strategy
 */
template <typename FailoverTry>
inline auto get_try_context(const FailoverTry& a_try) {
    auto res = detail::apply<get_try_context_impl>(unwrap(a_try));
    static_assert(HanaSequence<decltype(res)>,
        "get_try_context_impl::apply() should provide HanaSequence");
    return res;
}

template <typename Try>
struct get_next_try_impl {
    template <typename Conn>
    static auto apply(Try& a_try, error_code ec, Conn&& conn) {
        return a_try.get_next_try(ec, conn);
    }
};

/**
 * @brief Get the next try object
 *
 * Return #FailoverTry for next failover try if it possible. By default it calls
 * `a_try.get_next_try(ec, conn)`. This behavior customisable via `ozo::failover::get_next_try_impl`
 * specialization.
 *
 * @param a_try --- current #FailoverTry object.
 * @param ec --- current try error code.
 * @param conn --- current #connection object.
 * @return #FailoverTry --- if given error code and connection may be failovered with next try.
 * @return null-state --- otherwise.
 *
 * ###Customization Point
 *
 * This function may be customized for a #FailoverTry via specialization
 * of `ozo::failover::get_next_try_impl`. E.g.:
 * @code
namespace ozo::failover {

template <>
struct get_next_try_impl<my_strategy_try> {
    template <typename Allocator, typename ...Args>
    static std::optional<my_strategy_try> apply(const my_strategy_try& obj, error_code ec, Conn&& conn) {
        if (ec != my_error_condition::recoverable) {
            obj.log_error("can not recover error {0} with message {1}", ec, ozo::error_message(conn));
            return std::nullopt;
        }
        obj.log_notice("recovering error {0} with message {1}", ec, ozo::error_message(conn));
        return obj;
    }
};

} // namespace ozo::failover
 * @endcode
 * @ingroup group-failover-strategy
 */
template <typename Try, typename Connection>
inline auto get_next_try(Try& a_try, error_code ec, Connection&& conn) {
    return detail::apply<get_next_try_impl>(unwrap(a_try), std::move(ec),
        std::forward<Connection>(conn));
}

namespace detail {

template <template<typename...> typename Template, typename Allocator, typename ...Ts>
static auto allocate_shared(const Allocator& alloc, Ts&& ...vs) {
    using type = decltype(Template{std::forward<Ts>(vs)...});
    return std::allocate_shared<type>(alloc, std::forward<Ts>(vs)...);
}

template <typename Try, typename Operation, typename Handler>
inline void initiate_operation(const Operation&, Try&&, Handler&&);

template <typename Operation, typename Try, typename Handler>
struct continuation {
    Operation op_;
    Try try_;
    Handler handler_;

    continuation(const Operation& op, Try a_try, Handler handler)
    : op_(op), try_(std::move(a_try)), handler_(std::move(handler)) {}

    template <typename Connection>
    void operator() (error_code ec, Connection&& conn) {
        static_assert(ozo::Connection<Connection>, "conn should model Connection concept");
        if (ec) {
            auto next_try = get_next_try(try_, std::move(ec), std::forward<Connection>(conn));
            if (!is_null(next_try)) {
                initiate_operation(op_, std::move(next_try), std::move(handler_));
                return;
            }
        }
        handler_(std::move(ec), std::forward<Connection>(conn));
    }

    using executor_type = decltype(asio::get_associated_executor(handler_));

    executor_type get_executor() const {
        return asio::get_associated_executor(handler_);
    }

    using allocator_type = decltype(asio::get_associated_allocator(handler_));

    allocator_type get_allocator() const {
        return asio::get_associated_allocator(handler_);
    }
};

template <typename Try, typename Operation, typename Handler>
inline void initiate_operation(const Operation& op, Try&& a_try, Handler&& handler) {
    hana::unpack(get_try_context(a_try), [&](auto&& ...args) {
        ozo::get_operation_initiator(op)(
            continuation{op, std::forward<Try>(a_try), std::forward<Handler>(handler)},
            std::forward<decltype(args)>(args)...
        );
    });
}

template <typename Strategy, typename Operation>
struct operation_initiator {
    Strategy strategy_;
    Operation op_;

    constexpr operation_initiator(Strategy strategy, const Operation& op)
    : strategy_(std::move(strategy)), op_(op) {}

    template <typename Handler, typename ...Args>
    void operator() (Handler&& handler, Args&& ...args) const {
        auto first_try = get_first_try(op_, strategy_, asio::get_associated_allocator(handler), std::forward<Args>(args)...);
        initiate_operation(op_, std::move(first_try), std::forward<Handler>(handler));
    }
};

} // namespace detail

struct construct_initiator_impl {
    template <typename Strategy, typename Op>
    constexpr static auto apply(Strategy&& strategy, const Op& op) {
        return detail::operation_initiator(std::forward<Strategy>(strategy), op);
    }
};
} // namespace ozo::failover
