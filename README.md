# netman

基于 Rust 的网络管理守护进程，提供网卡配置（IPv4/IPv6、DHCP、DNS）、链路控制与事件订阅，通过 Unix socket 以 JSON-RPC 风格协议供 `nmctl` 客户端调用。

## 功能

- 查询网卡信息（硬件、IP 配置）
- 启用 / 禁用网口
- IPv4 / IPv6 配置（静态 / DHCP）
- 自动维护 `/etc/resolv.conf` DNS
- 配置持久化，重启自动恢复
- 基于 netlink 多播的实时网卡事件订阅

## 目录结构

```
src/
├── lib.rs              # 公共数据类型与 IPC 协议导出
├── main.rs             # 守护进程入口
├── ipc.rs              # Unix socket IPC（JSON-RPC 风格）
├── bin/nmctl.rs        # 命令行客户端
└── network/
    ├── mod.rs          # NetworkHandle 句柄
    ├── actor.rs        # 核心事件循环
    ├── netlink.rs      # rtnetlink 封装
    ├── monitor.rs      # netlink 多播监听
    ├── persist.rs      # 配置持久化
    ├── resolv.rs       # /etc/resolv.conf 管理
    └── dhcp/{v4,v6}.rs # DHCPv4 / DHCPv6 客户端
```

## 构建

```bash
cargo build --release
```

产物：`target/release/netman`（守护进程）与 `nmctl`（客户端）。

## 使用

启动守护进程（需 root）：

```bash
sudo ./target/release/netman
```

客户端命令：

```bash
nmctl list                       # 列出所有网卡
nmctl info eth0                  # 查看网卡详情
nmctl link eth0 up|down          # 启用/禁用网口
nmctl ipv4 eth0 --dhcp           # DHCP 自动获取
nmctl ipv4 eth0 --ip 192.168.1.10 --netmask 255.255.255.0 --gw 192.168.1.1
nmctl watch                      # 订阅网卡事件（Ctrl+C 退出）
```

> 💡 可用 `--socket /path/to/sock` 指定非默认 socket。

## 运行时说明

- **日志**：写入 `/var/log/netman.log`
- **持久化**：配置以 JSON 存放在 `/root/`，启动时自动加载
- **退出**：`Ctrl+C` 优雅退出并清理 socket

