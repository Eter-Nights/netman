# netman

基于 Rust 的网络管理库，提供网卡配置（IPv4/IPv6、DHCP、DNS）、链路控制与网卡事件订阅，随附一个交互式命令行工具 `nmctl`。

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
├── lib.rs              # 公共数据类型导出
├── network.rs          # NetworkHandle 句柄与命令/事件定义
├── bin/nmctl.rs        # 交互式命令行客户端（REPL）
└── network/
    ├── actor.rs        # 核心事件循环
    ├── netlink.rs      # rtnetlink 封装
    ├── monitor.rs      # netlink 多播监听
    ├── persist.rs      # 配置持久化
    ├── resolv.rs       # /etc/resolv.conf 管理
    └── dhcp/
        ├── mod.rs      # 模块声明
        ├── v4.rs       # DHCPv4 客户端
        └── v6.rs       # DHCPv6 客户端
```

## 构建

```bash
cargo build --release
```

产物：`target/release/nmctl`（单一可执行文件）。

## 使用

启动交互式 CLI（需 root）：

```bash
sudo ./target/release/nmctl
```

进入 REPL 后逐行输入命令：

```
netman> list                                   # 列出所有网卡
netman> info eth0                              # 查看网卡详情
netman> link eth0 up                           # 启用网口
netman> link eth0 down                         # 禁用网口
netman> ipv4 eth0 --dhcp                       # DHCP 自动获取
netman> ipv4 eth0 --ip 192.168.1.10 --netmask 255.255.255.0 --gw 192.168.1.1
netman> ipv6 eth0 --ip fd00::10 --prefix 64    # 静态 IPv6
netman> help                                   # 查看帮助
netman> quit                                   # 退出（Ctrl+C 亦可）
```

> 网卡事件（新增 / 删除 / 状态变化）会实时打印在终端中。

## 作为库使用

```rust
use netman::NetworkHandle;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let handle = NetworkHandle::new().await?;
    let cards = handle.get_all_netcards().await?;
    println!("{cards:#?}");
    Ok(())
}
```

## 运行时说明

- **日志**：输出到 stderr
- **持久化**：配置以 JSON 存放在 `/root/`，启动时自动加载

