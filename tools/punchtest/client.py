#!/usr/bin/env python3
"""UDP 打洞环境探针：客户端（在 NAT 后运行）。

用法: python3 client.py <名称> <信令服务器 ip:port> [本地端口]

两端同时运行；任一端收到对端 MAGIC 即判定打洞成功。
"""
import socket
import sys
import time

MAGIC = b"PUNCH-OK"


def main() -> None:
    name = sys.argv[1]
    server = sys.argv[2].rsplit(":", 1)
    server_addr = (server[0], int(server[1]))
    local_port = int(sys.argv[3]) if len(sys.argv) > 3 else 0

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", local_port))
    sock.settimeout(0.2)
    local = sock.getsockname()
    print(f"客户端 {name} 本地 {local[0]}:{local[1]}，信令 {server_addr[0]}:{server_addr[1]}")

    sock.sendto(f"REG {name}".encode(), server_addr)
    peer: tuple[str, int] | None = None
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            data, src = sock.recvfrom(1024)
        except socket.timeout:
            continue
        text = data.decode(errors="ignore").strip()
        if text.startswith("PEER "):
            _, peer_name, peer_addr_text = text.split(" ", 2)
            host, port = peer_addr_text.rsplit(":", 1)
            peer = (host, int(port))
            print(f"获取对端 {peer_name} 公网地址 {host}:{port}")
            break
    if peer is None:
        print("获取对端地址超时")
        return

    print(f"开始打洞探测（目标 {peer[0]}:{peer[1]}，最长 20 秒）")
    deadline = time.time() + 20
    while time.time() < deadline:
        sock.sendto(MAGIC, peer)
        try:
            data, src = sock.recvfrom(1024)
        except socket.timeout:
            continue
        if data == MAGIC:
            print(f"打洞成功！观测到对端真实来源 {src[0]}:{src[1]}")
            for _ in range(5):
                sock.sendto(MAGIC, src)
                sock.sendto(MAGIC, peer)
            return
    print("打洞失败：20 秒内未收到对端探测包")


if __name__ == "__main__":
    main()
