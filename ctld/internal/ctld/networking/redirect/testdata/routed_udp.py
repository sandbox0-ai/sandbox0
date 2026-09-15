"""Exercise actual generated rules against packets in disposable namespaces."""
import json
import os
import pathlib
import selectors
import socket
import struct
import subprocess
import sys


def run(*arguments, **kwargs):
    return subprocess.run(arguments, check=True, capture_output=True, text=True, timeout=5, **kwargs)


config = json.loads(pathlib.Path(sys.argv[1]).read_text())
namespace = "s0-udp-" + str(os.getpid())
run("ip", "netns", "add", namespace)
sockets = []
try:
    run("ip", "link", "add", "s0-host", "type", "veth", "peer", "name", "s0-guest")
    run("ip", "link", "set", "s0-guest", "netns", namespace)
    run("ip", "addr", "add", "192.0.2.1/24", "dev", "s0-host")
    run("ip", "link", "set", "s0-host", "up")
    run("ip", "link", "set", "lo", "up")
    for arguments in [
        ("addr", "add", "192.0.2.2/24", "dev", "s0-guest"),
        ("link", "set", "s0-guest", "up"),
        ("link", "set", "lo", "up"),
        ("route", "add", "default", "via", "192.0.2.1"),
    ]:
        run("ip", "-n", namespace, *arguments)
    run("ip", "rule", "add", "priority", "100", "fwmark", "1/1", "table", "100")
    run("ip", "route", "add", "local", "0.0.0.0/0", "dev", "lo", "table", "100")
    for table, chain in [("mangle", "CTLD_NETWORK_PREROUTING"), ("nat", "CTLD_NETWORK_NAT_PREROUTING")]:
        run("iptables", "-t", table, "-N", chain)
        run("iptables", "-t", table, "-A", "PREROUTING", "-j", chain)
    run("ipset", "restore", input=config["ipset"])
    run("iptables-restore", "--noflush", input=config["rules"])
    selector = selectors.DefaultSelector()
    for address, port, role in [
        ("0.0.0.0", 18080, "proxy"), ("0.0.0.0", 18443, "proxy"),
        ("192.0.2.1", 23460, "direct"), ("192.0.2.1", 443, "direct"), ("192.0.2.1", 853, "direct"),
    ]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sockets.append(sock)
        if role == "proxy":
            sock.setsockopt(socket.SOL_IP, 19, 1)  # IP_TRANSPARENT
            sock.setsockopt(socket.SOL_IP, 20, 1)  # IP_RECVORIGDSTADDR
        sock.bind((address, port))
        selector.register(sock, selectors.EVENT_READ, role)
    checks = 0
    for untracked in (False, True):
        if untracked:
            run("iptables", "-t", "raw", "-A", "PREROUTING", "-s", "192.0.2.2", "-p", "udp", "-j", "CT", "--notrack")
        for target in ("192.0.2.1", "198.51.100.1"):
            for port in (23460, 443, 853):
                token = str(checks)
                client = "import socket;s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);s.sendto(" + repr(token.encode()) + ",(" + repr(target) + "," + str(port) + "))"
                run("ip", "netns", "exec", namespace, "python3", "-c", client)
                events = selector.select(2)
                assert len(events) == 1, ("datagram did not reach exactly one listener", target, port, untracked)
                key, _ = events[0]
                payload, ancillary, _, peer = key.fileobj.recvmsg(256, 128)
                assert key.data == "proxy", ("policy bypass", target, port, untracked)
                assert payload == token.encode() and peer[0] == "192.0.2.2"
                destinations = [(socket.inet_ntoa(data[4:8]), struct.unpack("!H", data[2:4])[0])
                                for level, kind, data in ancillary if level == socket.SOL_IP and kind == 20]
                assert destinations == [(target, port)], ("original destination lost", destinations)
                checks += 1
    print(json.dumps({"routedUDPChecks": checks, "trackedAndUntracked": True, "originalDestinationsPreserved": True}))
finally:
    for sock in sockets:
        sock.close()
    run("ip", "netns", "delete", namespace)
