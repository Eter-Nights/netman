pub mod ipc;
mod network;

pub use ipc::{call, serve, DEFAULT_SOCKET_PATH, Envelope, Request, Response, RpcError};
pub use network::{NetCardEvent, NetworkHandle};

use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, Ipv6Addr};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkState {
    Down,           // 网口 down
    CableUnplugged, // 网口 up 但网线未插入
    Up,             // 网口 up 且插入网线
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HardwareInfo {
    pub ifindex: u32,     // 网口索引
    pub mac: [u8; 6],     // 网口 MAC 地址
    pub bandwidth: u32,   // 带宽（Mbps）
    pub state: LinkState, // 链路状态
}

impl HardwareInfo {
    pub fn new(ifindex: u32, mac: [u8; 6], bandwidth: u32, state: LinkState) -> Self {
        Self {
            ifindex,
            mac,
            bandwidth,
            state,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Ipv4Info {
    pub enabled: bool,           // 是否启用 IPv4 配置
    pub use_dhcp: bool,          // 是否使用 DHCP
    pub auto_dns: bool,          // 是否自动获取 DNS
    pub ip: Ipv4Addr,            // 静态 IP 地址
    pub netmask: Ipv4Addr,       // 子网掩码
    pub gateway: Ipv4Addr,       // 网关地址
    pub primary_dns: Ipv4Addr,   // 主 DNS
    pub secondary_dns: Ipv4Addr, // 备用 DNS
}

impl Ipv4Info {
    pub fn new(
        enabled: bool,
        use_dhcp: bool,
        auto_dns: bool,
        ip: Ipv4Addr,
        netmask: Ipv4Addr,
        gateway: Ipv4Addr,
        primary_dns: Ipv4Addr,
        secondary_dns: Ipv4Addr,
    ) -> Self {
        Self {
            enabled,
            use_dhcp,
            auto_dns,
            ip,
            netmask,
            gateway,
            primary_dns,
            secondary_dns,
        }
    }
}

impl Default for Ipv4Info {
    fn default() -> Self {
        Self {
            enabled: true,
            use_dhcp: true,
            auto_dns: true,
            ip: Ipv4Addr::UNSPECIFIED,
            netmask: Ipv4Addr::UNSPECIFIED,
            gateway: Ipv4Addr::UNSPECIFIED,
            primary_dns: Ipv4Addr::UNSPECIFIED,
            secondary_dns: Ipv4Addr::UNSPECIFIED,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Ipv6Info {
    pub enabled: bool,           // 是否启用 IPv6 配置
    pub use_dhcp: bool,          // 是否使用 DHCP
    pub auto_dns: bool,          // 是否自动获取 DNS
    pub ip: Ipv6Addr,            // 静态 IP 地址
    pub prefix_len: u8,          // 子网掩码前缀长度
    pub gateway: Ipv6Addr,       // 网关地址
    pub primary_dns: Ipv6Addr,   // 主 DNS
    pub secondary_dns: Ipv6Addr, // 备用 DNS
}

impl Ipv6Info {
    pub fn new(
        enabled: bool,
        use_dhcp: bool,
        auto_dns: bool,
        ip: Ipv6Addr,
        prefix_len: u8,
        gateway: Ipv6Addr,
        primary_dns: Ipv6Addr,
        secondary_dns: Ipv6Addr,
    ) -> Self {
        Self {
            enabled,
            use_dhcp,
            auto_dns,
            ip,
            prefix_len,
            gateway,
            primary_dns,
            secondary_dns,
        }
    }
}

impl Default for Ipv6Info {
    fn default() -> Self {
        Self {
            enabled: false,
            use_dhcp: true,
            auto_dns: true,
            ip: Ipv6Addr::UNSPECIFIED,
            prefix_len: 0,
            gateway: Ipv6Addr::UNSPECIFIED,
            primary_dns: Ipv6Addr::UNSPECIFIED,
            secondary_dns: Ipv6Addr::UNSPECIFIED,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetCardInfo {
    pub name: String,           // 网口名称
    pub enabled: bool,          // 网口开关
    pub hardware: HardwareInfo, // 硬件信息
    pub ipv4: Ipv4Info,         // IPv4 信息
    pub ipv6: Ipv6Info,         // IPv6 信息
}

impl NetCardInfo {
    pub fn new(
        name: String,
        enabled: bool,
        hardware: HardwareInfo,
        ipv4: Ipv4Info,
        ipv6: Ipv6Info,
    ) -> Self {
        Self {
            name,
            enabled,
            hardware,
            ipv4,
            ipv6,
        }
    }
}
