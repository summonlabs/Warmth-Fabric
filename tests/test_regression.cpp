// Warmth Fabric - regression tests: a worker that disconnects mid-warm settles
// its attempts, and stop() unblocks peers that never registered.
#include <atomic>
#include <chrono>
#include <thread>
#include "wtest.hpp"
#include "warmth/coordinator.hpp"
#include "warmth/messages.hpp"
#include "warmth/transport.hpp"

using namespace warmth;
using namespace warmth::proto;

namespace {
// Registers as a worker over framed TCP; true when the HELLO is accepted.
bool hello(net::TcpConnection& c, const Id128& wid, const Id128& boot) {
    Hello h; h.worker = wid; h.boot = boot; h.node = NodeId::derive("node"); h.backend = "cuda";
    WireEncoder w; encode_hello(w, h);
    Frame f; f.type = MessageType::HELLO; f.payload = w.done();
    FrameWriter fw(c);
    if (!fw.write(f)) return false;
    FrameReader fr(c);
    Frame reply;
    if (!fr.read(reply) || reply.type != MessageType::HELLO_ACK) return false;
    WireDecoder dec(reply.payload);
    HelloAck ack;
    return decode_hello_ack(dec, ack) && ack.accepted;
}
} // namespace

// A worker that disconnects before reporting leaves no attempt in flight and no
// budget reserved, so wait_until_settled() returns.
WTEST(disconnect_mid_warm_settles_attempts) {
    net::socket_init();
    Coordinator::Config cfg; cfg.host = "127.0.0.1";
    Coordinator coord(cfg);
    CHECK(coord.start());
    std::atomic<bool> stop{false};
    std::thread accept_thread([&] { coord.run_accept_loop(stop); });
    const auto wid = Id128::derive("regression-worker");
    net::TcpConnection worker = net::connect("127.0.0.1", coord.port());
    CHECK(worker.valid());
    CHECK(hello(worker, wid, Id128::derive("regression-boot")));   // registered before the ack is sent
    CHECK(coord.issue_warm(Id128::derive("regression-object"), WarmthAction::LOAD_MODEL, &wid));
    CHECK(coord.in_flight_attempts() == 1);
    worker.close();   // gone without a WARM_RESULT
    while (coord.registered_workers() != 0) std::this_thread::sleep_for(std::chrono::milliseconds(5));
    CHECK(coord.in_flight_attempts() == 0);
    CHECK(coord.balanced());
    if (coord.in_flight_attempts() == 0) coord.wait_until_settled();
    stop.store(true);
    coord.stop();
    accept_thread.join();
    net::socket_cleanup();
}

// stop() returns while a peer that never registered is still connected.
WTEST(stop_closes_unregistered_connections) {
    net::socket_init();
    Coordinator::Config cfg; cfg.host = "127.0.0.1";
    Coordinator coord(cfg);
    CHECK(coord.start());
    std::atomic<bool> stop{false};
    std::thread accept_thread([&] { coord.run_accept_loop(stop); });
    net::TcpConnection idle = net::connect("127.0.0.1", coord.port());
    CHECK(idle.valid());
    {   // one round trip: the coordinator is now serving this connection
        Frame f; f.type = MessageType::PING;
        FrameWriter fw(idle); CHECK(fw.write(f));
        FrameReader fr(idle); Frame r;
        CHECK(fr.read(r) && r.type == MessageType::PONG);
    }
    stop.store(true);
    coord.stop();
    accept_thread.join();
    idle.close();
    net::socket_cleanup();
}

// stop() racing the accept loop: a client connects and leaves just as the
// coordinator stops, many times over.
WTEST(stop_races_with_accept) {
    net::socket_init();
    for (int i = 0; i < 100; ++i) {
        Coordinator::Config cfg; cfg.host = "127.0.0.1";
        Coordinator coord(cfg);
        CHECK(coord.start());
        std::atomic<bool> stop{false};
        std::thread accept_thread([&] { coord.run_accept_loop(stop); });
        net::TcpConnection c = net::connect("127.0.0.1", coord.port());
        c.close();
        stop.store(true);
        coord.stop();
        accept_thread.join();
    }
    net::socket_cleanup();
}

// Pass a case name to run only that case.
int main(int argc, char** argv) {
    if (argc > 1) std::erase_if(wtest::registry(), [&](const wtest::Case& c) { return c.name != argv[1]; });
    RUN_TESTS();
}
