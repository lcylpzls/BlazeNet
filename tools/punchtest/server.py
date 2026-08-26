#!/usr/bin/env python3
"""UDP 打洞环境探针：信令服务器（公共 IP 上运行）。

用法: python3 server.py [端口，默认 30001]
"""
import socket
import sys


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 30001
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", port))
    clients: dict[str, tuple[str, int]] = {}
    print(f"信令服务器已启动: 0.0.0.0:{port}")
    while True:
        data, addr = sock.recvfrom(1024)
        text = data.decode(errors="ignore").strip()
        if text.startswith("REG "):
            name = text[4:]
            clients[name] = (addr[0], addr[1])
            print(f"注册: {name} -> {addr[0]}:{addr[1]}")
            if len(clients) >= 2:
                names = list(clients)
                for i, name_a in enumerate(names):
                    for name_b in names[i + 1 :]:
                        a = clients[name_a]
                        b = clients[name_b]
                        sock.sendto(f"PEER {name_b} {b[0]}:{b[1]}".encode(), a)
                        sock.sendto(f"PEER {name_a} {a[0]}:{a[1]}".encode(), b)
                        print(f"已交换: {name_a} <-> {name_b}")


if __name__ == "__main__":
    main()
