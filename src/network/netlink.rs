use crate::{HardwareInfo, LinkState};

use anyhow::Result;
use futures::StreamExt;
use rtnetlink::{
    AddressMessageBuilder, Handle, LinkUnspec, RouteMessageBuilder, new_connection,
    packet_route::link::{LinkAttribute, LinkFlags},
};
use std::fs;
use std::net::{Ipv4Addr, Ipv6Addr};

/// 网卡管理封装，集成 Netlink 操作与 DNS(/etc/resolv.conf) 管理
pub struct Netlink {
    handle: Handle,
}

impl Netlink {
    /// 创建新的 Netlink 连接，使用默认 /etc/resolv.conf
    pub fn new() -> Result<Self> {
        let (conn, handle, _) = new_connection()?;
        tokio::spawn(conn);
        Ok(Self { handle })
    }

    /// 启用网口 (up)
    pub async fn set_link_up(&self, iface_name: &str) -> Result<()> {
        let mut link_msg = LinkUnspec::new_with_name(iface_name);
        link_msg = link_msg.up();
        self.handle.link().set(link_msg.build()).execute().await?;
        Ok(())
    }

    /// 禁用网口 (down)
    pub async fn set_link_down(&self, iface_name: &str) -> Result<()> {
        let mut link_msg = LinkUnspec::new_with_name(iface_name);
        link_msg = link_msg.down();
        self.handle.link().set(link_msg.build()).execute().await?;
        Ok(())
    }

    /// 设置 IPv4 地址（若地址已存在则替换，幂等）
    pub async fn set_ipv4_address(
        &self,
        iface_name: &str,
        ip: Ipv4Addr,
        netmask: Ipv4Addr,
    ) -> Result<()> {
        let ifindex = self.get_ifindex_by_name(iface_name).await?;
        let prefix_len = u32::from(netmask).count_ones() as u8;
        self.handle
            .address()
            .add(ifindex, ip.into(), prefix_len)
            .replace()
            .execute()
            .await?;

        Ok(())
    }

    /// 删除 IPv4 地址
    pub async fn del_ipv4_address(
        &self,
        iface_name: &str,
        ip: Ipv4Addr,
        netmask: Ipv4Addr,
    ) -> Result<()> {
        let ifindex = self.get_ifindex_by_name(iface_name).await?;
        let prefix_len = u32::from(netmask).count_ones() as u8;
        let addr_msg = AddressMessageBuilder::<Ipv4Addr>::new()
            .index(ifindex)
            .address(ip, prefix_len)
            .build();

        self.handle.address().del(addr_msg).execute().await?;

        Ok(())
    }

    /// 设置 IPv6 地址（若地址已存在则替换，幂等）
    pub async fn set_ipv6_address(
        &self,
        iface_name: &str,
        ip: Ipv6Addr,
        prefix_len: u8,
    ) -> Result<()> {
        let ifindex = self.get_ifindex_by_name(iface_name).await?;
        self.handle
            .address()
            .add(ifindex, ip.into(), prefix_len)
            .replace()
            .execute()
            .await?;

        Ok(())
    }

    /// 删除 IPv6 地址
    pub async fn del_ipv6_address(
        &self,
        iface_name: &str,
        ip: Ipv6Addr,
        prefix_len: u8,
    ) -> Result<()> {
        let ifindex = self.get_ifindex_by_name(iface_name).await?;
        let addr_msg = AddressMessageBuilder::<Ipv6Addr>::new()
            .index(ifindex)
            .address(ip, prefix_len)
            .build();

        self.handle.address().del(addr_msg).execute().await?;

        Ok(())
    }

    /// 设置 IPv4 默认网关（若已存在等价路由则替换，幂等）
    pub async fn set_ipv4_gateway(&self, iface_name: &str, gateway: Ipv4Addr) -> Result<()> {
        let ifindex = self.get_ifindex_by_name(iface_name).await?;
        let route_msg = RouteMessageBuilder::<Ipv4Addr>::new()
            .destination_prefix(Ipv4Addr::UNSPECIFIED, 0)
            .gateway(gateway)
            .output_interface(ifindex)
            .build();

        self.handle
            .route()
            .add(route_msg)
            .replace()
            .execute()
            .await?;

        Ok(())
    }

    /// 删除 IPv4 默认网关
    pub async fn del_ipv4_gateway(&self, iface_name: &str, gateway: Ipv4Addr) -> Result<()> {
        let ifindex = self.get_ifindex_by_name(iface_name).await?;
        let route_msg = RouteMessageBuilder::<Ipv4Addr>::new()
            .destination_prefix(Ipv4Addr::UNSPECIFIED, 0)
            .gateway(gateway)
            .output_interface(ifindex)
            .build();

        self.handle.route().del(route_msg).execute().await?;

        Ok(())
    }

    /// 设置 IPv6 默认网关（若已存在等价路由则替换，幂等）
    pub async fn set_ipv6_gateway(&self, iface_name: &str, gateway: Ipv6Addr) -> Result<()> {
        let ifindex = self.get_ifindex_by_name(iface_name).await?;
        let route_msg = RouteMessageBuilder::<Ipv6Addr>::new()
            .destination_prefix(Ipv6Addr::UNSPECIFIED, 0)
            .gateway(gateway)
            .output_interface(ifindex)
            .build();

        self.handle
            .route()
            .add(route_msg)
            .replace()
            .execute()
            .await?;

        Ok(())
    }

    /// 删除 IPv6 默认网关
    pub async fn del_ipv6_gateway(&self, iface_name: &str, gateway: Ipv6Addr) -> Result<()> {
        let ifindex = self.get_ifindex_by_name(iface_name).await?;
        let route_msg = RouteMessageBuilder::<Ipv6Addr>::new()
            .destination_prefix(Ipv6Addr::UNSPECIFIED, 0)
            .gateway(gateway)
            .output_interface(ifindex)
            .build();

        self.handle.route().del(route_msg).execute().await?;

        Ok(())
    }

    /// 获取所有网卡的名称列表（过滤掉 loopback 等非物理接口）
    pub async fn get_all_iface_names(&self) -> Result<Vec<String>> {
        let mut result = Vec::new();
        let mut link_stream = self.handle.link().get().execute();

        while let Some(link_msg) = link_stream.next().await {
            let link_msg = link_msg?;
            // 跳过 loopback 接口（如 lo）：DHCP 客户端无法在其上初始化，
            // 管理它也没有意义。
            if link_msg.header.flags.contains(LinkFlags::Loopback) {
                continue;
            }
            for attr in link_msg.attributes {
                if let LinkAttribute::IfName(n) = attr {
                    result.push(n);
                    break;
                }
            }
        }

        Ok(result)
    }

    /// 获取单张网卡的 HardwareInfo
    pub async fn get_hardware_info(&self, iface_name: &str) -> Result<HardwareInfo> {
        let link_msg = self
            .handle
            .link()
            .get()
            .match_name(iface_name.to_string())
            .execute()
            .next()
            .await;

        let link_msg = match link_msg {
            Some(Ok(msg)) => msg,
            Some(Err(e)) => return Err(e.into()),
            None => {
                return Err(anyhow::anyhow!("Interface '{}' not found", iface_name));
            }
        };

        let ifindex = link_msg.header.index;
        let flags = link_msg.header.flags;
        let mut mac = [0u8; 6];

        for attr in link_msg.attributes {
            if let LinkAttribute::Address(addr) = attr
                && addr.len() == 6
            {
                mac.copy_from_slice(&addr);
            }
        }

        let path = format!("/sys/class/net/{}/speed", iface_name);
        let bandwidth = fs::read_to_string(&path)
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .unwrap_or(0);

        // 根据 flags 确定链路状态
        let state = if !flags.contains(LinkFlags::Up) {
            LinkState::Down
        } else if !flags.contains(LinkFlags::Running) {
            LinkState::CableUnplugged
        } else {
            LinkState::Up
        };

        let hardware = HardwareInfo::new(ifindex, mac, bandwidth, state);
        Ok(hardware)
    }

    /// 根据网卡名称获取 ifindex
    async fn get_ifindex_by_name(&self, name: &str) -> Result<u32> {
        let link_msg = self
            .handle
            .link()
            .get()
            .match_name(name.to_string())
            .execute()
            .next()
            .await;

        match link_msg {
            Some(Ok(msg)) => Ok(msg.header.index),
            Some(Err(e)) => Err(e.into()),
            None => Err(anyhow::anyhow!("Interface '{}' not found", name)),
        }
    }
}
