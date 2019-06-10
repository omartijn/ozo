#include <ozo/io/send.h>
#include <ozo/io/array.h>
#include <ozo/ext/std.h>
#include <ozo/ext/boost/uuid.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <string_view>

namespace {

using namespace testing;

struct send : Test {
    std::vector<char> buffer;
    ozo::detail::ostreambuf obuf{buffer};
    ozo::ostream os{&obuf};

    struct badbuf_t : std::streambuf{} badbuf;
    ozo::ostream bad_ostream{&badbuf};

    ozo::empty_oid_map oid_map;
};

TEST_F(send, with_single_byte_type_and_bad_ostream_should_throw) {
    EXPECT_THROW(
        ozo::send(bad_ostream, oid_map, char(42)),
        ozo::system_error
    );
}

TEST_F(send, with_multi_byte_type_and_bad_ostream_should_throw) {
    EXPECT_THROW(
        ozo::send(bad_ostream, oid_map, std::int64_t(42)),
        ozo::system_error
    );
}

TEST_F(send, with_std_int8_t_should_store_it_as_is) {
    ozo::send(os, oid_map, char(42));
    EXPECT_THAT(buffer, ElementsAre(42));
}

TEST_F(send, with_std_int16_t_should_store_it_in_big_endian_order) {
    ozo::send(os, oid_map, std::int16_t(42));
    EXPECT_THAT(buffer, ElementsAre(0, 42));
}

TEST_F(send, with_std_int32_t_should_store_it_in_big_endian_order) {
    ozo::send(os, oid_map, std::int32_t(42));
    EXPECT_THAT(buffer, ElementsAre(0, 0, 0, 42));
}

TEST_F(send, with_std_int64_t_should_store_it_in_big_endian_order) {
    ozo::send(os, oid_map, std::int64_t(42));
    EXPECT_THAT(buffer, ElementsAre(0, 0, 0, 0, 0, 0, 0, 42));
}

TEST_F(send, with_float_should_store_it_as_integral_in_big_endian_order) {
    ozo::send(os, oid_map, 42.13f);
    EXPECT_THAT(buffer, ElementsAre(0x42, 0x28, 0x85, 0x1F));
}

TEST_F(send, with_std_string_should_store_it_as_is) {
    ozo::send(os, oid_map, std::string("text"));
    EXPECT_THAT(buffer, ElementsAre('t', 'e', 'x', 't'));
}

TEST_F(send, with_std_string_view_should_store_it_as_is) {
    using namespace std::string_view_literals;
    ozo::send(os, oid_map, "view"sv);
    EXPECT_THAT(buffer, ElementsAre('v', 'i', 'e', 'w'));
}

TEST_F(send, with_std_vector_of_float_should_store_with_one_dimension_array_header_and_values) {
    ozo::send(os, oid_map, std::vector<float>({42.13f}));
    EXPECT_EQ(buffer, std::vector<char>({
        0, 0, 0, 1,
        0, 0, 0, 0,
        0, 0, 2, '\xBC',
        0, 0, 0, 1,
        0, 0, 0, 0,
        0, 0, 0, 4,
        0x42, 0x28, char(0x85), 0x1F,
    }));
}

TEST_F(send, with_std_array_of_int_should_store_with_one_dimension_array_header_and_values) {
    ozo::send(os, oid_map, std::array<int, 3>{{1, 2, 3}});
    EXPECT_EQ(buffer, std::vector<char>({
        0, 0, 0, 1,
        0, 0, 0, 0,
        0, 0, 0, 0x17,
        0, 0, 0, 3,
        0, 0, 0, 0,
        0, 0, 0, 4,
        0, 0, 0, 0x1,
        0, 0, 0, 4,
        0, 0, 0, 0x2,
        0, 0, 0, 4,
        0, 0, 0, 0x3,
    }));
}

TEST_F(send, should_send_nothing_for_std_nullptr_t) {
    ozo::send(os, oid_map, nullptr);
    EXPECT_TRUE(buffer.empty());
}

TEST_F(send, should_send_nothing_for_std_nullopt_t) {
    ozo::send(os, oid_map, __OZO_NULLOPT);
    EXPECT_TRUE(buffer.empty());
}

TEST(send_impl, should_send_nothing_for_std_nullptr_t) {
    std::vector<char> buffer;
    ozo::detail::ostreambuf obuf{buffer};
    ozo::ostream os{&obuf};
    ozo::empty_oid_map oid_map;

    ozo::send_impl<std::nullptr_t>::apply(os, oid_map, nullptr);
    EXPECT_TRUE(buffer.empty());
}

TEST(send_impl, should_send_nothing_for_std_nullopt_t) {
    std::vector<char> buffer;
    ozo::detail::ostreambuf obuf{buffer};
    ozo::ostream os{&obuf};
    ozo::empty_oid_map oid_map;

    ozo::send_impl<__OZO_NULLOPT_T>::apply(os, oid_map, __OZO_NULLOPT);
    EXPECT_TRUE(buffer.empty());
}

struct send_frame : Test {
    using buffer_t = std::vector<char>;
    buffer_t buffer;
    ozo::detail::ostreambuf obuf{buffer};
    ozo::ostream os{&obuf};

    ozo::empty_oid_map oid_map;
    auto buffer_range(int pos, int count) {
        const auto first = buffer.begin() + pos;
        const auto end = first + count;
        return boost::make_iterator_range(first, end);
    }

    auto oid_buffer() {
        return buffer_range(0, 4);
    }

    auto size_buffer() {
        return buffer_range(4, 4);
    }

    auto data_buffer() {
        return boost::make_iterator_range(buffer.begin() + 8, buffer.end());
    }
};

TEST_F(send_frame, should_write_pg_bytea_as_binary_byte_buffer) {
    ozo::send_frame(os, oid_map, ozo::pg::bytea({0,1,2,3,4,5,6,7,8,9,0}));
    EXPECT_THAT(oid_buffer(), ElementsAreArray({0x00, 0x00, 0x00, 0x11}));
    EXPECT_THAT(size_buffer(), ElementsAreArray({0x00, 0x00, 0x00, 0x0B}));
    EXPECT_THAT(data_buffer(), ElementsAreArray({0,1,2,3,4,5,6,7,8,9,0}));
}

TEST_F(send_frame, should_write_pg_name_as_string) {
    ozo::send_frame(os, oid_map, ozo::pg::name {"name"});
    EXPECT_THAT(oid_buffer(), ElementsAreArray({0x00, 0x00, 0x00, 0x13}));
    EXPECT_THAT(size_buffer(), ElementsAreArray({0x00, 0x00, 0x00, 0x04}));
    EXPECT_THAT(data_buffer(), ElementsAre('n', 'a', 'm', 'e'));
}

TEST_F(send, with_boost_uuid_should_store_it_as_is) {
    const boost::uuids::uuid uuid = {
        0x12, 0x34, 0x56, 0x78,
        0x90, 0xab, 0xcd, 0xef,
        0x12, 0x34, 0x56, 0x78,
        0x40, 0xab, 0xcd, 0xef
     };
    ozo::send(os, oid_map, uuid);
    EXPECT_EQ(buffer, std::vector<char>({
        0x12, 0x34, 0x56, 0x78,
        char(0x90), char(0xab), char(0xcd), char(0xef),
        0x12, 0x34, 0x56, 0x78,
        0x40, char(0xab), char(0xcd), char(0xef)
    }));
}

} // namespace
