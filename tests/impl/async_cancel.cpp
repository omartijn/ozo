#include <connection_mock.h>
#include <test_error.h>

#include <ozo/impl/async_cancel.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

namespace {

using namespace std::literals;
using namespace ::testing;

struct dispatch_cancel : Test {
    struct cancel_handle_mock {
        using self_type = cancel_handle_mock;
        MOCK_METHOD1(pq_cancel, bool(std::string& ));

        friend bool pq_cancel(self_type* self, std::string& err) {
            return self->pq_cancel(err);
        }
    } handle;
};

TEST_F(dispatch_cancel, should_return_no_error_and_empty_string_if_pq_cancel_returns_true) {
    EXPECT_CALL(handle, pq_cancel(_)).WillOnce(Return(true));
    auto [ec, msg] = ozo::impl::dispatch_cancel(&handle);
    EXPECT_FALSE(ec);
    EXPECT_TRUE(msg.empty());
}

TEST_F(dispatch_cancel, should_return_pq_cancel_failed_and_non_empty_string_if_pq_cancel_returns_false_and_sets_message) {
    EXPECT_CALL(handle, pq_cancel(_)).WillOnce(Invoke([](std::string& msg){
        msg = "error message";
        return false;
    }));
    auto [ec, msg] = ozo::impl::dispatch_cancel(&handle);
    EXPECT_EQ(ozo::error_code{ozo::error::pq_cancel_failed}, ec);
    EXPECT_FALSE(msg.empty());
}

TEST_F(dispatch_cancel, should_remove_trailing_zeroes_from_error_message) {
    EXPECT_CALL(handle, pq_cancel(_)).WillOnce(Invoke([](std::string& msg){
        msg = "error message\0\0\0\0\0\0\0\0\0\0";
        return false;
    }));
    auto [ec, msg] = ozo::impl::dispatch_cancel(&handle);
    EXPECT_TRUE(ec);
    EXPECT_EQ(msg, "error message"s);
}

struct cancel_op_handler : Test {
    struct connection_mock{
        MOCK_METHOD1(set_error_context, void(std::string));
        MOCK_METHOD0(close_connection, void());

        friend void set_error_context(connection_mock* self, std::string msg) {
            self->set_error_context(msg);
        }

        friend void close_connection(connection_mock* self) {
            self->close_connection();
        }
    } connection;

    testing::StrictMock<ozo::tests::callback_gmock<connection_mock*>> handler;
};

TEST_F(cancel_op_handler, should_call_handler_only_once) {
    ozo::impl::cancel_op_handler h{std::addressof(connection), ozo::tests::wrap(handler)};
    EXPECT_CALL(handler, call(_,_)).WillOnce(Return());
    h({}, {});
    h({}, {});
}

TEST_F(cancel_op_handler, should_set_error_context_and_close_connection_if_called_with_error) {
    ozo::impl::cancel_op_handler h{std::addressof(connection), ozo::tests::wrap(handler)};
    EXPECT_CALL(connection, set_error_context("error message")).WillOnce(Return());
    EXPECT_CALL(connection, close_connection()).WillOnce(Return());
    EXPECT_CALL(handler, call(_,_)).WillOnce(Return());
    h(ozo::tests::error::error, "error message");
}

struct async_cancel : Test {
    struct strand_service : ozo::tests::strand_executor_service_mock {
        ozo::tests::executor_gmock executor;
        ozo::tests::executor_mock& get_executor() {
            return executor;
        }
    } strand;

    ozo::tests::executor_gmock executor;
    ozo::tests::io_context io{executor, strand};
    ozo::tests::executor_gmock system_executor;
    ozo::tests::io_context sys_execution_ctx{system_executor};
    ozo::tests::executor_gmock cb_executor;
    ozo::tests::io_context cb_io{cb_executor};


    struct cancel_handle_mock {
        MOCK_METHOD0(dispatch_cancel, std::tuple<ozo::error_code, std::string>());

        friend auto dispatch_cancel(cancel_handle_mock* self) {
            return self->dispatch_cancel();
        }
    } cancel_handle;

    ozo::tests::steady_timer_gmock timer;

    struct connection_mock {
        ozo::tests::steady_timer timer;
        ozo::tests::io_context& io;

        connection_mock(ozo::tests::steady_timer_mock& timer, ozo::tests::io_context& io)
        : timer{std::addressof(timer)}, io(io) {}

        MOCK_METHOD1(set_error_context, void(std::string));
        MOCK_METHOD0(close_connection, void());
        MOCK_METHOD0(get_cancel_handle, cancel_handle_mock*());

        template <typename Ctx>
        friend void set_error_context(connection_mock* self, Ctx&& msg) {
            self->set_error_context(std::forward<Ctx>(msg));
        }

        friend void close_connection(connection_mock* self) {
            self->close_connection();
        }

        friend ozo::tests::steady_timer& get_timer(connection_mock* self) {
            return self->timer;
        }

        friend cancel_handle_mock* get_cancel_handle(connection_mock* self) {
            return self->get_cancel_handle();
        }

        friend auto get_executor(connection_mock* self) {
            return self->io.get_executor();
        }

    } connection {timer, io};

    testing::StrictMock<ozo::tests::callback_gmock<connection_mock*>> callback;
};

}

namespace ozo {
template<>
struct is_connection<::async_cancel::connection_mock*> : std::true_type {};
}
namespace {

TEST_F(async_cancel, should_dispatch_handler_with_error_if_get_cancel_handle_returns_null) {
    EXPECT_CALL(connection, get_cancel_handle()).WillOnce(Return(nullptr));
    EXPECT_CALL(connection, set_error_context(_)).WillOnce(Return());
    EXPECT_CALL(cb_executor, dispatch(_)).WillOnce(InvokeArgument<0>());
    EXPECT_CALL(callback, call(ozo::error_code{ozo::error::pq_get_cancel_failed}, _)).WillOnce(Return());

    ozo::impl::async_cancel(
            std::addressof(connection),
            sys_execution_ctx.get_executor(),
            ozo::time_traits::duration::max(),
            wrap(callback, cb_io.get_executor())
    );
}

TEST_F(async_cancel, should_dispatch_cancel_operation_in_given_executor_and_callback_with_no_error_on_success) {
    InSequence s;
    EXPECT_CALL(connection, get_cancel_handle()).WillOnce(Return(std::addressof(cancel_handle)));
    EXPECT_CALL(timer, expires_after(_)).WillOnce(Return(0));
    EXPECT_CALL(timer, async_wait(_)).WillOnce(Return());
    EXPECT_CALL(system_executor, post(_)).WillOnce(InvokeArgument<0>());

    EXPECT_CALL(cancel_handle, dispatch_cancel())
        .WillOnce(Return(std::make_tuple(ozo::error_code{}, std::string{})));

    EXPECT_CALL(strand.executor, dispatch(_)).WillOnce(InvokeArgument<0>());

    EXPECT_CALL(timer, cancel()).WillOnce(Return(1));

    EXPECT_CALL(executor, post(_)).WillOnce(InvokeArgument<0>());
    EXPECT_CALL(cb_executor, dispatch(_)).WillOnce(InvokeArgument<0>());
    EXPECT_CALL(callback, call(ozo::error_code{}, _)).WillOnce(Return());

    ozo::impl::async_cancel(
            std::addressof(connection),
            sys_execution_ctx.get_executor(),
            ozo::time_traits::duration::max(),
            wrap(callback, cb_io.get_executor())
    );
}

TEST_F(async_cancel, should_callback_with_operation_aborted_error_on_timer_timeout) {
    InSequence s;
    EXPECT_CALL(connection, get_cancel_handle()).WillOnce(Return(std::addressof(cancel_handle)));
    EXPECT_CALL(timer, expires_after(_)).WillOnce(Return(0));

    std::function<void(ozo::error_code)> timer_handler;
    EXPECT_CALL(timer, async_wait(_)).WillOnce(Invoke([&](auto h) {timer_handler = h;}));
    EXPECT_CALL(system_executor, post(_)).WillOnce(Return());

    EXPECT_CALL(strand.executor, post(_)).WillOnce(InvokeArgument<0>());
    EXPECT_CALL(connection, set_error_context(_)).WillOnce(Return());
    EXPECT_CALL(connection, close_connection()).WillOnce(Return());
    EXPECT_CALL(timer, cancel()).WillOnce(Return(0));

    EXPECT_CALL(executor, post(_)).WillOnce(InvokeArgument<0>());
    EXPECT_CALL(cb_executor, dispatch(_)).WillOnce(InvokeArgument<0>());
    EXPECT_CALL(callback, call(ozo::error_code{boost::asio::error::operation_aborted}, _))
        .WillOnce(Return());

    ozo::impl::async_cancel(
            std::addressof(connection),
            sys_execution_ctx.get_executor(),
            ozo::time_traits::duration::max(),
            wrap(callback, cb_io.get_executor())
    );
    timer_handler(ozo::error_code{});
}

TEST_F(async_cancel, should_callback_with_error_and_set_error_context_on_dispatch_cancel_error) {
    InSequence s;
    EXPECT_CALL(connection, get_cancel_handle()).WillOnce(Return(std::addressof(cancel_handle)));
    EXPECT_CALL(timer, expires_after(_)).WillOnce(Return(0));
    EXPECT_CALL(timer, async_wait(_)).WillOnce(Return());
    EXPECT_CALL(system_executor, post(_)).WillOnce(InvokeArgument<0>());

    EXPECT_CALL(cancel_handle, dispatch_cancel())
        .WillOnce(Return(std::make_tuple(ozo::error_code{ozo::tests::error::error}, "error message"s)));

    EXPECT_CALL(strand.executor, dispatch(_)).WillOnce(InvokeArgument<0>());
    EXPECT_CALL(connection, set_error_context("error message"s)).WillOnce(Return());
    EXPECT_CALL(connection, close_connection()).WillOnce(Return());

    EXPECT_CALL(timer, cancel()).WillOnce(Return(1));

    EXPECT_CALL(executor, post(_)).WillOnce(InvokeArgument<0>());
    EXPECT_CALL(cb_executor, dispatch(_)).WillOnce(InvokeArgument<0>());
    EXPECT_CALL(callback, call(ozo::error_code{ozo::tests::error::error}, _)).WillOnce(Return());

    ozo::impl::async_cancel(
            std::addressof(connection),
            sys_execution_ctx.get_executor(),
            ozo::time_traits::duration::max(),
            wrap(callback, cb_io.get_executor())
    );
}

}
