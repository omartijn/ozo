#include "benchmark.h"

#include <ozo/connection_info.h>
#include <ozo/connection_pool.h>
#include <ozo/request.h>
#include <ozo/query_builder.h>

#include <nlohmann/json.hpp>

#include <boost/asio/io_service.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/program_options.hpp>

#include <condition_variable>
#include <thread>

namespace {

namespace asio = boost::asio;

using benchmark_t = ozo::benchmark::time_limit_benchmark;

constexpr const std::chrono::seconds connect_timeout(1);
constexpr const std::chrono::seconds request_timeout(1);
constexpr const ozo::connection_pool_timeouts pool_timeouts {std::chrono::seconds(1), std::chrono::seconds(1)};

template <typename T>
void spawn(asio::io_context& io, std::size_t token, T&& coroutine) {
    asio::spawn(io, [token, coroutine = std::forward<T>(coroutine)] (asio::yield_context yield) {
        try {
            coroutine(yield);
        } catch (const boost::coroutines::detail::forced_unwind&) {
            throw;
        } catch (const std::exception& e) {
            std::cerr << "coroutine " << token << " failed: " << e.what() << std::endl;
        }
    });
}

enum class query_type {
    simple,
    complex,
};

std::ostream& operator <<(std::ostream& stream, query_type value) {
    switch (value) {
        case query_type::simple:
            return stream << "simple";
        case query_type::complex:
            return stream << "complex";
    }
    return stream;
}

std::istream& operator >>(std::istream& stream, query_type& value) {
    std::string token;
    stream >> token;
    if (token == "simple") {
        value = query_type::simple;
    } else if (token == "complex") {
        value = query_type::complex;
    } else {
        throw std::invalid_argument("Invalid query type: \"" + token + "\"");
    }
    return stream;
}

struct benchmark_params {
    std::string conn_string;
    ::query_type query_type;
    std::chrono::steady_clock::duration duration;
    std::size_t coroutines;
    std::size_t threads_number;
    std::size_t queue_capacity;
    std::size_t connections;
    bool verbose;
};

struct benchmark_report {
    std::string name;
    std::string query;
    ozo::benchmark::output output;
    ozo::benchmark::stats stats;
    __OZO_STD_OPTIONAL<std::size_t> coroutines;
    __OZO_STD_OPTIONAL<std::size_t> threads_number;
    __OZO_STD_OPTIONAL<std::size_t> queue_capacity;
    __OZO_STD_OPTIONAL<std::size_t> connections;
};

std::ostream& operator <<(std::ostream& stream, const benchmark_report& value) {
    stream << "benchmark: " << value.name << '\n';
    stream << "query: " << value.query << '\n';
    if (value.coroutines) {
        stream << "threads_number: " << *value.coroutines << '\n';
    }
    if (value.threads_number) {
        stream << "threads_number: " << *value.threads_number << '\n';
    }
    if (value.queue_capacity) {
        stream << "queue_capacity: " << *value.queue_capacity << '\n';
    }
    if (value.connections) {
        stream << "connections: " << *value.connections << '\n';
    }
    stream << value.stats << '\n';
    return stream;
}

template <typename Query>
benchmark_report reuse_connection_info(const benchmark_params& params, Query query) {
    benchmark_report report;
    report.name = __func__;
    report.query = ozo::to_const_char(ozo::get_text(query));

    benchmark_t benchmark(1, params.duration);
    asio::io_context io(1);
    ozo::connection_info<> connection_info(params.conn_string);

    benchmark.set_print_progress(params.verbose);

    spawn(io, 0, [&] (asio::yield_context yield) {
        while (true) {
            ozo::result result;
            const auto provider = ozo::make_connector(connection_info, io, connect_timeout);
            ozo::error_code ec;
            auto connection = ozo::request(provider, query, request_timeout, std::ref(result), yield[ec]);
            if (ec) {
                if (connection) {
                    std::cerr << ozo::get_error_context(connection) << '\n';
                    std::cerr << ozo::error_message(connection) << '\n';
                }
                throw boost::system::system_error(ec);
            }
            if (!benchmark.step(result.size())) {
                break;
            }
        }
    });

    io.run();

    report.output = benchmark.get_output();
    report.stats = benchmark.get_stats();

    return report;
}

template <typename Result, typename Query>
benchmark_report reuse_connection_info_and_parse_result(const benchmark_params& params, Query query) {
    benchmark_report report;
    report.name = __func__;
    report.query = ozo::to_const_char(ozo::get_text(query));

    benchmark_t benchmark(1, params.duration);
    asio::io_context io(1);
    ozo::connection_info<> connection_info(params.conn_string);

    benchmark.set_print_progress(params.verbose);

    spawn(io, 0, [&] (asio::yield_context yield) {
        while (true) {
            std::vector<Result> result;
            const auto provider = ozo::make_connector(connection_info, io, connect_timeout);
            ozo::error_code ec;
            auto connection = ozo::request(provider, query, request_timeout, std::back_inserter(result), yield[ec]);
            if (ec) {
                if (connection) {
                    std::cerr << ozo::get_error_context(connection) << '\n';
                    std::cerr << ozo::error_message(connection) << '\n';
                }
                throw boost::system::system_error(ec);
            }
            if (!benchmark.step(result.size())) {
                break;
            }
        }
    });

    io.run();

    report.output = benchmark.get_output();
    report.stats = benchmark.get_stats();

    return report;
}

template <typename Query>
benchmark_report reuse_connection(const benchmark_params& params, Query query) {
    benchmark_report report;
    report.name = __func__;
    report.query = ozo::to_const_char(ozo::get_text(query));

    benchmark_t benchmark(1, params.duration);
    asio::io_context io(1);
    ozo::connection_info<> connection_info(params.conn_string);

    benchmark.set_print_progress(params.verbose);

    spawn(io, 0, [&] (asio::yield_context yield) {
        const auto provider = ozo::make_connector(connection_info, io, connect_timeout);
        auto connection = ozo::get_connection(provider, yield);
        while (true) {
            ozo::result result;
            ozo::error_code ec;
            auto connection = ozo::request(provider, query, request_timeout, std::ref(result), yield[ec]);
            if (ec) {
                if (connection) {
                    std::cerr << ozo::get_error_context(connection) << '\n';
                    std::cerr << ozo::error_message(connection) << '\n';
                }
                throw boost::system::system_error(ec);
            }
            if (!benchmark.step(result.size())) {
                break;
            }
        }
    });

    io.run();

    report.output = benchmark.get_output();
    report.stats = benchmark.get_stats();

    return report;
}

template <typename Result, typename Query>
benchmark_report reuse_connection_and_parse_result(const benchmark_params& params, Query query) {
    benchmark_report report;
    report.name = __func__;
    report.query = ozo::to_const_char(ozo::get_text(query));

    benchmark_t benchmark(1, params.duration);
    asio::io_context io(1);
    ozo::connection_info<> connection_info(params.conn_string);

    benchmark.set_print_progress(params.verbose);

    spawn(io, 0, [&] (asio::yield_context yield) {
        const auto provider = ozo::make_connector(connection_info, io, connect_timeout);
        auto connection = ozo::get_connection(provider, yield);
        while (true) {
            std::vector<Result> result;
            ozo::error_code ec;
            auto connection = ozo::request(provider, query, request_timeout, std::back_inserter(result), yield[ec]);
            if (ec) {
                if (connection) {
                    std::cerr << ozo::get_error_context(connection) << '\n';
                    std::cerr << ozo::error_message(connection) << '\n';
                }
                throw boost::system::system_error(ec);
            }
            if (!benchmark.step(result.size())) {
                break;
            }
        }
    });

    io.run();

    report.output = benchmark.get_output();
    report.stats = benchmark.get_stats();

    return report;
}

template <typename Query>
benchmark_report use_connection_pool(const benchmark_params& params, Query query) {
    benchmark_report report;
    report.name = __func__;
    report.query = ozo::to_const_char(ozo::get_text(query));

    benchmark_t benchmark(1, params.duration);
    asio::io_context io(1);
    const ozo::connection_info<> connection_info(params.conn_string);
    ozo::connection_pool_config config;
    config.capacity = 2;
    config.queue_capacity = 0;
    auto pool = ozo::make_connection_pool(connection_info, config);

    benchmark.set_print_progress(params.verbose);

    spawn(io, 0, [&] (asio::yield_context yield) {
        while (true) {
            auto provider = ozo::make_connector(pool, io, pool_timeouts);
            ozo::result result;
            ozo::error_code ec;
            auto connection = ozo::request(provider, query, request_timeout, std::ref(result), yield[ec]);
            if (ec) {
                if (connection) {
                    std::cerr << ozo::get_error_context(connection) << '\n';
                    std::cerr << ozo::error_message(connection) << '\n';
                }
                throw boost::system::system_error(ec);
            }
            if (!benchmark.step(result.size())) {
                break;
            }
        }
    });

    io.run();

    report.output = benchmark.get_output();
    report.stats = benchmark.get_stats();

    return report;
}

template <typename Result, typename Query>
benchmark_report use_connection_pool_and_parse_result(const benchmark_params& params, Query query) {
    benchmark_report report;
    report.name = __func__;
    report.query = ozo::to_const_char(ozo::get_text(query));

    benchmark_t benchmark(1, params.duration);
    asio::io_context io(1);
    const ozo::connection_info<> connection_info(params.conn_string);
    ozo::connection_pool_config config;
    config.capacity = 2;
    config.queue_capacity = 0;
    auto pool = ozo::make_connection_pool(connection_info, config);

    benchmark.set_print_progress(params.verbose);

    spawn(io, 0, [&] (asio::yield_context yield) {
        while (true) {
            auto provider = ozo::make_connector(pool, io, pool_timeouts);
            std::vector<Result> result;
            ozo::error_code ec;
            auto connection = ozo::request(provider, query, request_timeout, std::back_inserter(result), yield[ec]);
            if (ec) {
                if (connection) {
                    std::cerr << ozo::get_error_context(connection) << '\n';
                    std::cerr << ozo::error_message(connection) << '\n';
                }
                throw boost::system::system_error(ec);
            }
            if (!benchmark.step(result.size())) {
                break;
            }
        }
    });

    io.run();

    report.output = benchmark.get_output();
    report.stats = benchmark.get_stats();

    return report;
}

template <typename Query>
benchmark_report use_connection_pool_mult_connection(const benchmark_params& params, Query query) {
    benchmark_report report;
    report.name = __func__;
    report.query = ozo::to_const_char(ozo::get_text(query));
    report.coroutines = params.coroutines;
    report.queue_capacity = params.queue_capacity;

    benchmark_t benchmark(params.coroutines, params.duration);
    asio::io_context io(1);
    const ozo::connection_info<> connection_info(params.conn_string);
    ozo::connection_pool_config config;
    config.capacity = params.connections;
    config.queue_capacity = params.queue_capacity;
    auto pool = ozo::make_connection_pool(connection_info, config);

    benchmark.set_print_progress(params.verbose);

    for (std::size_t token = 0; token < params.coroutines; ++token) {
        spawn(io, 0, [&, token] (asio::yield_context yield) {
            while (true) {
                auto provider = ozo::make_connector(pool, io, pool_timeouts);
                ozo::result result;
                ozo::error_code ec;
                auto connection = ozo::request(provider, query, request_timeout, std::ref(result), yield[ec]);
                if (ec) {
                    if (connection) {
                        std::cerr << ozo::get_error_context(connection) << '\n';
                        std::cerr << ozo::error_message(connection) << '\n';
                    }
                    throw boost::system::system_error(ec);
                }
                if (!benchmark.step(result.size(), token)) {
                    break;
                }
            }
        });
    }

    io.run();

    report.output = benchmark.get_output();
    report.stats = benchmark.get_stats();

    return report;
}

template <typename Result, typename Query>
benchmark_report use_connection_pool_and_parse_result_mult_connection(const benchmark_params& params, Query query) {
    benchmark_report report;
    report.name = __func__;
    report.query = ozo::to_const_char(ozo::get_text(query));
    report.coroutines = params.coroutines;
    report.queue_capacity = params.queue_capacity;

    benchmark_t benchmark(params.coroutines, params.duration);
    asio::io_context io(1);
    const ozo::connection_info<> connection_info(params.conn_string);
    ozo::connection_pool_config config;
    config.capacity = params.coroutines;
    config.queue_capacity = params.queue_capacity;
    auto pool = ozo::make_connection_pool(connection_info, config);

    benchmark.set_print_progress(params.verbose);

    for (std::size_t token = 0; token < params.coroutines; ++token) {
        spawn(io, token, [&, token] (asio::yield_context yield) {
            while (true) {
                auto provider = ozo::make_connector(pool, io, pool_timeouts);
                std::vector<Result> result;
                ozo::error_code ec;
                auto connection = ozo::request(provider, query, request_timeout, std::back_inserter(result), yield[ec]);
                if (ec) {
                    if (connection) {
                        std::cerr << ozo::get_error_context(connection) << '\n';
                        std::cerr << ozo::error_message(connection) << '\n';
                    }
                    throw boost::system::system_error(ec);
                }
                if (!benchmark.step(result.size(), token)) {
                    break;
                }
            }
        });
    }

    io.run();

    report.output = benchmark.get_output();
    report.stats = benchmark.get_stats();

    return report;
}

struct context {
    asio::io_context io;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> guard = boost::asio::make_work_guard(io);
    std::thread thread {[this] { this->io.run(); }};

    context() = default;
};

template <typename Query>
benchmark_report use_connection_pool_mult_threads(const benchmark_params& params, Query query) {
    benchmark_report report;
    report.name = __func__;
    report.query = ozo::to_const_char(ozo::get_text(query));
    report.coroutines = params.coroutines;
    report.queue_capacity = params.queue_capacity;
    report.threads_number = params.threads_number;
    report.connections = params.connections;

    benchmark_t benchmark(params.threads_number * params.coroutines, params.duration);
    const ozo::connection_info<> connection_info(params.conn_string);
    ozo::connection_pool_config config;
    config.capacity =  params.connections;
    config.queue_capacity = params.queue_capacity;
    std::vector<std::unique_ptr<context>> contexts;
    auto pool = ozo::make_connection_pool(connection_info, config);
    std::atomic_size_t finished_coroutines {0};
    std::mutex mutex;
    std::unique_lock<std::mutex> lock(mutex);
    std::condition_variable coroutine_finished;

    benchmark.set_print_progress(params.verbose);

    for (std::size_t i = 0; i < params.threads_number; ++i) {
        contexts.emplace_back(std::make_unique<context>());
        auto& io = contexts.back()->io;
        for (std::size_t j = 0; j < params.coroutines; ++j) {
            const auto token = params.coroutines * i + j;
            spawn(io, token, [&, token] (asio::yield_context yield) {
                while (true) {
                    auto provider = ozo::make_connector(pool, io, pool_timeouts);
                    ozo::result result;
                    boost::system::error_code ec;
                    auto connection = ozo::request(provider, query, request_timeout, std::ref(result), yield[ec]);
                    if (!benchmark.thread_safe_step(result.size(), token)) {
                        break;
                    }
                    if (ec) {
                        if (connection) {
                            std::cerr << ozo::get_error_context(connection) << '\n';
                            std::cerr << ozo::error_message(connection) << '\n';
                        }
                        break;
                    }
                }
                ++finished_coroutines;
                coroutine_finished.notify_all();
            });
        }
    }

    if (params.threads_number * params.coroutines > 0) {
        coroutine_finished.wait(lock, [&] { return finished_coroutines >= params.threads_number * params.coroutines; });
    }

    std::for_each(contexts.begin(), contexts.end(), [] (const auto& v) { v->guard.reset(); });
    std::for_each(contexts.begin(), contexts.end(), [] (const auto& v) { v->thread.join(); });

    report.output = benchmark.get_output();
    report.stats = benchmark.get_stats();

    return report;
}

template <typename Result, typename Query>
benchmark_report run_benchmark(const std::string& name, const benchmark_params& params, Query query) {
    std::map<std::string, std::function<benchmark_report ()>> scenarios {{
        {
            "reuse_connection_info",
            [&] { return reuse_connection_info(params, query); }
        },
        {
            "reuse_connection_info_and_parse_result",
            [&] { return reuse_connection_info_and_parse_result<Result>(params, query); }
        },
        {
            "reuse_connection",
            [&] { return reuse_connection(params, query); }
        },
        {
            "reuse_connection_and_parse_result",
            [&] { return reuse_connection_and_parse_result<Result>(params, query); }
        },
        {
            "use_connection_pool",
            [&] { return use_connection_pool(params, query); }
        },
        {
            "use_connection_pool_and_parse_result",
            [&] { return use_connection_pool_and_parse_result<Result>(params, query); }
        },
        {
            "use_connection_pool_mult_threads",
            [&] { return use_connection_pool_mult_threads(params, query); }
        }
    }};

    const auto scenario = scenarios.find(name);

    if (scenario == scenarios.end()) {
        throw std::invalid_argument("Invalid benchmark name: \"" + name + "\"");
    }

    return scenario->second();
}

benchmark_report run_benchmark(const std::string& name, const benchmark_params& params) {
    using namespace ozo::literals;

    const auto simple_query = "SELECT 1"_SQL.build();
    const auto complex_query = (
        "SELECT typname, typnamespace, typowner, typlen, typbyval, typcategory, "_SQL +
        "typispreferred, typisdefined, typdelim, typrelid, typelem, typarray "_SQL +
        "FROM pg_type WHERE typtypmod = "_SQL + -1 + " AND typisdefined = "_SQL + true
    ).build();

    switch (params.query_type) {
        case query_type::simple:
            return run_benchmark<std::tuple<std::int64_t>>(name, params, simple_query);
        case query_type::complex:
            return run_benchmark<ozo::benchmark::pg_type>(name, params, complex_query);
    }

    throw std::invalid_argument("Invalid query type: \"" + std::to_string(static_cast<int>(params.query_type)) + "\"");
}

enum class format {
    text,
    json
};

std::ostream& operator <<(std::ostream& stream, format value) {
    switch (value) {
        case format::text:
            return stream << "text";
        case format::json:
            return stream << "json";
    }
    return stream;
}

std::istream& operator >>(std::istream& stream, format& value) {
    std::string token;
    stream >> token;
    if (token == "text") {
        value = format::text;
    } else if (token == "json") {
        value = format::json;
    } else {
        throw std::invalid_argument("Invalid format: \"" + token + "\"");
    }
    return stream;
}

} // namespace

namespace nlohmann {

template <>
struct adl_serializer<std::chrono::steady_clock::duration> {
    static void to_json(json& j, const std::chrono::steady_clock::duration& value) {
        j = value.count();
    }

    static void from_json(const json&, std::chrono::steady_clock::duration&) {
        throw std::logic_error("std::chrono::steady_clock::duration is not implemented");
    }
};

template <>
struct adl_serializer<ozo::benchmark::stats> {
    static void to_json(json& j, const ozo::benchmark::stats& value) {
        if (value.mean_request_time) {
            j["mean_request_time"] = *value.mean_request_time;
        }
        if (value.median_request_time) {
            j["median_request_time"] = *value.median_request_time;
        }
        if (value.q90_request_time) {
            j["q90_request_time"] = *value.q90_request_time;
        }
        if (value.min_request_time) {
            j["min_request_time"] = *value.min_request_time;
        }
        if (value.max_request_time) {
            j["max_request_time"] = *value.max_request_time;
        }
        j["mean_request_speed"] = value.mean_request_speed;
        if (value.median_request_speed) {
            j["median_request_speed"] = *value.median_request_speed;
        }
        if (value.min_request_speed) {
            j["min_request_speed"] = *value.min_request_speed;
        }
        if (value.max_request_speed) {
            j["max_request_speed"] = *value.max_request_speed;
        }
        j["mean_read_rows_speed"] = value.mean_read_rows_speed;
        if (value.median_read_rows_speed) {
            j["median_read_rows_speed"] = *value.median_read_rows_speed;
        }
        if (value.min_read_rows_speed) {
            j["min_read_rows_speed"] = *value.min_read_rows_speed;
        }
        if (value.max_read_rows_speed) {
            j["max_read_rows_speed"] = *value.max_read_rows_speed;
        }
    }

    static void from_json(const json&, ozo::benchmark::stats&) {
        throw std::logic_error("ozo::benchmark::stats serialization is not implemented");
    }
};

template <>
struct adl_serializer<ozo::benchmark::step> {
    static void to_json(json& j, const ozo::benchmark::step& value) {
        j["duration"] = value.duration;
        j["rows_count"] = value.rows_count;
        j["requests_count"] = value.requests_count;
    }

    static void from_json(const json&, ozo::benchmark::step&) {
        throw std::logic_error("ozo::benchmark::step serialization is not implemented");
    }
};

template <>
struct adl_serializer<ozo::benchmark::output> {
    static void to_json(json& j, const ozo::benchmark::output& value) {
        j["steps"] = value.steps;
        j["requests"] = value.requests;
    }

    static void from_json(const json&, ozo::benchmark::output&) {
        throw std::logic_error("ozo::benchmark::output serialization is not implemented");
    }
};

template <>
struct adl_serializer<benchmark_report> {
    static void to_json(json& j, const benchmark_report& value) {
        j["name"] = value.name;
        j["query"] = value.query;
        if (value.coroutines) {
            j["coroutines"] = *value.coroutines;
        }
        if (value.connections) {
            j["connections"] = *value.connections;
        }
        if (value.queue_capacity) {
            j["queue_capacity"] = *value.queue_capacity;
        }
        if (value.threads_number) {
            j["threads_number"] = *value.threads_number;
        }
        j["output"] = value.output;
        j["stats"] = value.stats;
    }

    static void from_json(const json&, benchmark_report&) {
        throw std::logic_error("benchmark_report serialization is not implemented");
    }
};

} // namespace nlohmann

int main(int argc, char **argv) {
    using namespace ozo::benchmark;
    using namespace hana::literals;

    namespace po = boost::program_options;

    try {
        po::options_description options;

        options.add_options()
            ("help,h", "print help message")
            ("benchmark,b", po::value<std::string>(), "benchmark name to run")
            ("verbose,v", "use verbose output")
            ("format,f", po::value<format>()->default_value(format::text), "benchmark report format (text, json)")
            ("duration,d", po::value<std::uint64_t>()->default_value(31), "benchmark duration in seconds")
            ("coroutines", po::value<std::size_t>()->default_value(1), "number of parallel coroutines")
            ("connections", po::value<std::size_t>(), "number of parallel coroutines (default: equal to coroutines + 1)")
            ("threads", po::value<std::size_t>()->default_value(1), "number of threads")
            ("queue", po::value<std::size_t>()->default_value(0), "connection pool queue capacity")
            ("conninfo", po::value<std::string>()->default_value(""), "psql-like database connection info")
            ("query", po::value<query_type>()->default_value(query_type::simple), "query type (simple or complex)")
        ;

        po::variables_map variables;
        po::store(po::parse_command_line(argc, argv, options), variables);
        po::notify(variables);

        if (variables.count("help")) {
            std::cout << options << std::endl;
            return 0;
        }

        if (!variables.count("benchmark")) {
            std::cerr << "Nothing to run: benchmark is not set" << std::endl;
            return -1;
        }

        benchmark_params params;
        params.conn_string = variables.at("conninfo").as<std::string>();
        params.query_type = variables.at("query").as<query_type>();
        params.coroutines = variables.at("coroutines").as<std::size_t>();
        params.queue_capacity = variables.at("queue").as<std::size_t>();
        params.threads_number = variables.at("threads").as<std::size_t>();
        if (variables.count("connections")) {
            params.connections = variables.at("connections").as<std::size_t>();
        } else {
            params.connections = params.coroutines;
        }
        params.verbose = variables.count("verbose");
        params.duration = std::chrono::seconds(variables.at("duration").as<std::uint64_t>());

        const auto report = run_benchmark(variables.at("benchmark").as<std::string>(), params);

        switch (variables.at("format").as<format>()) {
            case format::text:
                std::cout << report << std::endl;
                break;
            case format::json:
                std::cout << nlohmann::json(report) << std::endl;
                break;
        }

        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
}
