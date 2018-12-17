#pragma once

#include <ozo/connection.h>
#include <ozo/time_traits.h>

namespace ozo {
#ifdef OZO_DOCUMENTATION
/**
 * @brief Cancel execution of current request on a database backend.
 *
 * Sometimes you need to cancel request to a database due to operation time constraint.
 * In case of closing connection, PostgreSQL backend will continue to execute request anyway.
 * So to prevent such waste of database resources it is best practice to cancel the execution
 * of the request by sending special command.
 *
 * @note The function does not particitate in ADL since could be implemented via functional object.
 *
 * @param connection --- #Connection with the active operation to cancel, note the connection would be closed.
 * @param executor --- Executor on which the operation will be executed.
 * @param time_constraint --- time constraint to wait for the operation complete.
 * @param token --- operation #CompletionToken.
 * @return deduced from #CompletionToken.
 *
 *
 * @note Given connection will be closed any way before the operation completion token handler will be called.
 *
 * Since libpq's cancel operation implementation is synchronous only it would block a thread where executed.
 * That's why it needs a dedicated Executor. User should specify an executor to implement a proper execution
 * strategy, e.g. the operations' queue which would be handled in a dedicated thread and so on.
 *
 * @note If a timer hits the specified time constraint only waiting process would be canceled.
 * The cancel operation itself would continue to execute since there is no way to cancel it.
 * User should take it into account planning to use specified executor.
 *
 * @ingroup group-requests-functions
 */
template <typename Connection, typename Executor, typename TimeConstraint, typename CompletionToken>
auto cancel(Connection&& connection, const Executor& executor, TimeConstraint time_constraint, CompletionToken&& token);

/**
 * @brief Cancel execution of current request on a database backend.
 *
 * This version executes cancel operation on `boost::asio::system_executor` on unspecified thread pool.
 *
 * @note The function does not particitate in ADL since could be implemented via functional object.
 *
 * @param connection --- #Connection with active operation to cancel.
 * @param time_constraint --- time constraint to waiting for cancel operation.
 * @param token --- operation #CompletionToken.
 * @return deduced from #CompletionToken.
 *
 * Since libpq's cancel operation implementation is synchronous only, the default behaviour is to execute
 * the operation via `boost::asio::system_executor` in a separate thread for each call. User may specify own executor,
 * to implement different strategy, e.g. cancel operation queue which would be handled in a dedicated thread and so on.
 *
 * @ingroup group-requests-functions
 */
template <typename Connection, typename TimeConstraint, typename CompletionToken>
auto cancel(Connection&& connection, TimeConstraint time_constraint, CompletionToken&& token);

/**
 * @brief Cancel execution of current request on a database backend.
 *
 * This version executes cancel operation on `boost::asio::system_executor` on unspecified thread pool without
 * a time constraint.
 *
 * @note The function does not particitate in ADL since could be implemented via functional object.
 *
 * @param connection --- #Connection with active operation to cancel.
 * @param token --- operation #CompletionToken.
 * @return deduced from #CompletionToken.
 *
 * @note Use it very carefull because of libpq's synchronous implementation there is no way
 * to cancel IO via a #Connection socket. The only option is to stop wait.
 *
 * @ingroup group-requests-functions
 */
template <typename Connection, typename CompletionToken>
auto cancel(Connection&& connection, CompletionToken&& token);
#else
struct cancel_op {
    template <typename Connection, typename Executor, typename TimeConstraint, typename CompletionToken>
    auto operator()(Connection&& connection, const Executor& executor, TimeConstraint time_constraint,
            CompletionToken&& token) const {

        static_assert(ozo::Connection<Connection>, "connection should model Connection");
        static_assert(ozo::TimeConstraint<TimeConstraint>, "time_constraint should model TimeConstrain");

        using signature_t = void (error_code, std::decay_t<connection>);
        async_completion<CompletionToken, signature_t> init(token);

        impl::async_cancel(std::forward<Connection>(connection), executor, time_constraint, init.completion_handler);

        return init.result.get();
    }

    template <typename Connection, typename TimeConstraint, typename CompletionToken>
    auto operator()(Connection&& connection, TimeConstraint time_constraint, CompletionToken&& token) const {
        return (*this)(
            std::forward<Connection>(connection),
            asio::system_executor{},
            time_constraint,
            std::forward<CompletionToken>(token)
        );
    }

    template <typename Connection, typename CompletionToken>
    auto operator()(Connection&& connection, CompletionToken&& token) const {
        return cancel(
            std::forward<Connection>(connection),
            asio::system_executor{},
            ozo::none,
            std::forward<CompletionToken>(token)
        );
    }
};

constexpr cancel_op cancel;

#endif
} // namespace ozo
