//
// Created by matt on 23/2/21.
//

#include <doctest/doctest.h>
#include <caf/all.hpp>
#include <caf/io/all.hpp>
#include <utility>
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <normal/tuple/arrow/Arrays.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/tuple/serialization/ArrowSer.h>
#include "Globals.h"

#define SKIP_SUITE false

using namespace caf;
using namespace normal::tuple;

/**
 * A test message. Contains a pointer to an arrow table plus methods for converting to and from a byte vector.
 */
class TestMessage {
public:
    explicit TestMessage() = default;

    explicit TestMessage(std::shared_ptr<::arrow::Table> table) : table_(std::move(table)) {};

    [[nodiscard]] const std::shared_ptr<::arrow::Table> &getTable() const {
        return table_;
    }

    void setTable(const std::shared_ptr<::arrow::Table> &table) {
        table_ = table;
    }

    [[nodiscard]] std::vector<std::uint8_t> toBytes() const {
        return table_to_bytes(table_);
    }

    void fromBytes(const std::vector<std::uint8_t> &bytes_vec) {
        table_ = bytes_to_table(bytes_vec);
    }

private:
    std::shared_ptr<::arrow::Table> table_;
};

/**
 * A CAF type inspector telling CAF to serialize TestMessage by using the byte vector conversion methods.
 *
 * @tparam Inspector
 * @param f
 * @param x
 * @return
 */
template<class Inspector>
bool inspect(Inspector &f, TestMessage &x) {
    auto toBytes = [&x]() -> decltype(auto) {
        return x.toBytes();
    };
    auto fromBytes = [&x](const std::vector<std::uint8_t> &value) {
        x.fromBytes(value);
        return true;
    };
    return f.object(x).fields(f.field("table_bytes", toBytes, fromBytes));
}

/**
 * Definitions for serializable types
 */
CAF_BEGIN_TYPE_ID_BLOCK(normal, caf::first_custom_type_id)
CAF_ADD_TYPE_ID(normal, (TestMessage))
CAF_END_TYPE_ID_BLOCK(normal)

/**
 * Actor definitions, Just a ping and a pong actor that bounce a test message between each other.
 */
using pong_actor = typed_actor<
        reacts_to<put_atom, TestMessage>>;

using ping_actor = typed_actor<
        reacts_to<connect_atom, actor>,
        reacts_to<put_atom, TestMessage>>;

struct ping_state {
    pong_actor pong;
    static inline const char *name = "ping";
};

struct pong_state {
    static inline const char *name = "pong";
};


std::shared_ptr<arrow::Table> makeTestTable(){
    auto schema = ::arrow::schema({
                                          {field("f0", ::arrow::int32())},
                                          {field("f1", ::arrow::int32())},
                                          {field("f2", ::arrow::int32())},
                                  });

    auto array_0 = Arrays::make<::arrow::Int32Type>({1, 2, 3}).value();
    auto array_1 = Arrays::make<::arrow::Int32Type>({4, 5, 6}).value();
    auto array_2 = Arrays::make<::arrow::Int32Type>({7, 8, 9}).value();

    auto table = ::arrow::Table::Make(schema, {array_0, array_1, array_2});

    return table;
}

/**
 * Pings behaviour. Just sends a test message to pong. Should receive it back again and quit.
 * @param self
 * @return
 */
ping_actor::behavior_type ping_behaviour(ping_actor::stateful_pointer <ping_state> self) {

    SPDLOG_INFO("ping spawned");

    return {
            [=](connect_atom, const actor &p_actor) {
                self->state.pong = actor_cast<pong_actor>(p_actor);
                auto table = makeTestTable();
                SPDLOG_INFO("ping received connect message, sending table  |  table: {}", normal::tuple::TupleSet2(table).showString(normal::tuple::TupleSetShowOptions(normal::tuple::TupleSetShowOrientation::RowOriented)));
                self->send(self->state.pong, put_atom_v, TestMessage(table));
            },
            [=](put_atom, const TestMessage &message) {
                SPDLOG_INFO("ping received test message from pong  |  table: {}", normal::tuple::TupleSet2(message.getTable()).showString(normal::tuple::TupleSetShowOptions(normal::tuple::TupleSetShowOrientation::RowOriented)));
                self->quit();
            }
    };
}

/**
 * Pongs behaviour. Just sends a received test message back to the sender and quits.
 * @param self
 * @return
 */
pong_actor::behavior_type pong_behaviour(pong_actor::stateful_pointer <pong_state> self) {

    SPDLOG_INFO("pong spawned");

    return {
            [=](put_atom, const TestMessage &message) {
                SPDLOG_INFO("pong received test message from ping, sending back table  |  Table: {}", normal::tuple::TupleSet2(message.getTable()).showString(normal::tuple::TupleSetShowOptions(normal::tuple::TupleSetShowOrientation::RowOriented)));
                self->anon_send(caf::actor_cast<caf::actor>(self->current_sender()), put_atom_v, message);
                self->quit();
            }
    };
}

/**
 * Starts an actor system on separate threads, one to host ping, and one to host pong. The test case then
 * connects to both and grabs the actor handles, sending the pong handle to ping. It then waits for them to
 * finish.
 */
TEST_CASE ("remote-serialization" * doctest::skip(true || SKIP_SUITE)) {

//    auto inTable = makeTestTable();
//    SPDLOG_INFO("In table: {}", normal::tuple::TupleSet2(inTable).showString(normal::tuple::TupleSetShowOptions(normal::tuple::TupleSetShowOrientation::RowOriented)));
//
//    auto bytes = table_to_bytes(inTable);
//
//    SPDLOG_INFO("Bytes read");
//    for (auto val : bytes) printf("\\x%.2x", val);
//    SPDLOG_INFO("");
//
//    inTable.reset();
//
//    SPDLOG_INFO("Bytes read after freeing table ptr");
//    for (auto val : bytes) printf("\\x%.2x", val);
//    SPDLOG_INFO("");
//
//    auto outTable = bytes_to_table(bytes);
//    SPDLOG_INFO("Out table: {}", normal::tuple::TupleSet2(outTable).showString(normal::tuple::TupleSetShowOptions(normal::tuple::TupleSetShowOrientation::RowOriented)));

    auto p1 = std::promise<bool>();
    auto f1 = p1.get_future();

    std::thread t1([&]() {
        caf::init_global_meta_objects<caf::id_block::normal>();
        caf::core::init_global_meta_objects();
        caf::io::middleman::init_global_meta_objects();
        caf::actor_system_config cfg;
        cfg.load<caf::io::middleman>();
        caf::actor_system actor_system(cfg);
        auto ping = actor_system.spawn(ping_behaviour);
        auto maybe_actor = actor_system.middleman().publish(ping, 40001, "127.0.0.1");
        if (!maybe_actor) {
            SPDLOG_ERROR("Error  |  error: {}", to_string(maybe_actor));
            p1.set_value(false);
        }
        p1.set_value(true);
        actor_system.await_all_actors_done();
    });

    auto p2 = std::promise<bool>();
    auto f2 = p2.get_future();

    std::thread t2([&]() {
        caf::init_global_meta_objects<caf::id_block::normal>();
        caf::core::init_global_meta_objects();
        caf::io::middleman::init_global_meta_objects();
        caf::actor_system_config cfg;
        cfg.load<caf::io::middleman>();
        caf::actor_system actor_system(cfg);
        auto pong = actor_system.spawn(pong_behaviour);
        auto maybe_actor = actor_system.middleman().publish(pong, 40002, "127.0.0.1");
        if (!maybe_actor) {
            SPDLOG_ERROR("Error  |  error: {}", to_string(maybe_actor));
            p2.set_value(false);
        }
        p2.set_value(true);
        actor_system.await_all_actors_done();
    });

    f1.wait();
    f2.wait();

    caf::init_global_meta_objects<caf::id_block::normal>();
    caf::core::init_global_meta_objects();
    caf::io::middleman::init_global_meta_objects();
    caf::actor_system_config cfg;
    cfg.load<caf::io::middleman>();
    caf::actor_system actor_system(cfg);

    auto maybe_ping = actor_system.middleman().remote_actor<ping_actor>("127.0.0.1", 40001);
    if (!maybe_ping)
        SPDLOG_ERROR("Error connecting to ping  |  error: {}", to_string(maybe_ping));
    SPDLOG_INFO("Connected to ping");

    auto maybe_pong = actor_system.middleman().remote_actor<pong_actor>("127.0.0.1", 40002);
    if (!maybe_pong)
        SPDLOG_ERROR("Error connecting to pong  |  error: {}", to_string(maybe_pong));
    SPDLOG_INFO("Connected to pong");

    scoped_actor self{actor_system};
    self->anon_send(maybe_ping.value(), connect_atom_v, caf::actor_cast<actor>(maybe_pong.value()));

    t1.join();
    t2.join();
}

TEST_CASE ("arrow-serialization-chunkedArray" * doctest::skip(false || SKIP_SUITE)) {
  arrow::Int32Builder arrayBuilder(arrow::default_memory_pool());
  std::vector<int> values{1, 2, 3, 4, 5};
  auto status = arrayBuilder.AppendValues(values);
  std::shared_ptr<arrow::Array> array;
  status = arrayBuilder.Finish(&array);
  auto chunkedArray = std::make_shared<arrow::ChunkedArray>(array);
  SPDLOG_INFO("Original chunkedArray: Length = {}\nContent: {}", chunkedArray->length(), chunkedArray->ToString());

  auto bytes = chunkedArray_to_bytes(chunkedArray);
  auto newChunkedArray = bytes_to_chunkedArray(bytes);
  SPDLOG_INFO("New chunkedArray: Length = {}\nContent: {}", newChunkedArray->length(), newChunkedArray->ToString());
}
