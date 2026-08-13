use super::{Command, DhcpEvent, NetCardEvent};
use super::{
    dhcp::{Dhcp4Client, Dhcp6Client},
    monitor::{LinkEvent, LinkMonitor},
    netlink::Netlink,
    persist, resolv,
};
use crate::{Ipv4Info, Ipv6Info, LinkState, NetCardInfo};

use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// 网络管理器（内部实现）。
pub(super) struct NetworkActor {
    netlink: Netlink,
    netcards: HashMap<String, NetCardInfo>,
    dhcp4_clients: HashMap<String, Dhcp4Client>,
    dhcp6_clients: HashMap<String, Dhcp6Client>,
    link_monitor: LinkMonitor,

    dhcp_tx: mpsc::Sender<DhcpEvent>,     // DHCP 租约发送
    dhcp_rx: mpsc::Receiver<DhcpEvent>,   // DHCP 租约接收
    link_rx: mpsc::Receiver<LinkEvent>,   // 网卡状态事件接收
    cmd_rx: mpsc::Receiver<Command>,      // 外部命令接收
    event_tx: mpsc::Sender<NetCardEvent>, // 网卡事件上报
}

impl NetworkActor {
    /// 创建网络管理器。
    pub(super) async fn new(
        cmd_rx: mpsc::Receiver<Command>,
        event_tx: mpsc::Sender<NetCardEvent>,
    ) -> Result<Self, anyhow::Error> {
        let netlink = Netlink::new()?;

        let (link_tx, link_rx) = mpsc::channel(32);
        let link_monitor = LinkMonitor::new(link_tx);

        let (dhcp_tx, dhcp_rx) = mpsc::channel(32);

        let mut actor = Self {
            netlink,
            netcards: HashMap::new(),
            dhcp4_clients: HashMap::new(),
            dhcp6_clients: HashMap::new(),
            link_monitor,

            dhcp_tx,
            dhcp_rx,
            link_rx,
            cmd_rx,
            event_tx,
        };

        if let Err(e) = actor.init_netcards().await {
            error!("Failed to initialize netcards: {}", e);
        }

        Ok(actor)
    }

    /// 初始化网卡信息缓存
    async fn init_netcards(&mut self) -> Result<(), anyhow::Error> {
        let names = self.netlink.get_all_iface_names().await?;

        for iface_name in names {
            // 单张网卡初始化失败时跳过，避免影响其它网卡的加载
            if let Err(e) = self.add_netcard(iface_name).await {
                warn!("Failed to initialize netcard, skipping: {}", e);
            }
        }

        Ok(())
    }

    /// 新增单张网卡到缓存：拉取硬件信息，创建 NetCardInfo 与对应的 DHCP 客户端。
    async fn add_netcard(&mut self, iface_name: String) -> Result<(), anyhow::Error> {
        let hardware = self.netlink.get_hardware_info(&iface_name).await?;

        self.dhcp4_clients.insert(
            iface_name.clone(),
            Dhcp4Client::new(iface_name.clone(), self.dhcp_tx.clone()).await?,
        );
        self.dhcp6_clients.insert(
            iface_name.clone(),
            Dhcp6Client::new(iface_name.clone(), self.dhcp_tx.clone()).await?,
        );

        // 读取持久化配置；不存在则用默认值
        let saved = persist::load(&iface_name);
        let link_up = saved.as_ref().map(|c| c.link_up()).unwrap_or(true);
        let ipv4 = saved.as_ref().map(|c| c.ipv4()).unwrap_or_default();
        let ipv6 = saved.as_ref().map(|c| c.ipv6()).unwrap_or_default();

        let netcard = NetCardInfo::new(
            iface_name.clone(),
            link_up,
            hardware,
            ipv4.clone(),
            ipv6.clone(),
        );
        self.netcards.insert(iface_name.clone(), netcard);

        // 按持久化配置下发 link 状态与 IP 配置
        if saved.is_some() {
            if let Err(e) = self.set_link_state(&iface_name, link_up).await {
                warn!("Failed to apply link state for iface {}: {}", iface_name, e);
            }
            // 仅当网卡为 up 时才下发 IP 配置，down 状态下配 IP 无意义
            if link_up {
                if let Err(e) = self.set_ipv4_info(&iface_name, ipv4).await {
                    warn!(
                        "Failed to apply ipv4 config for iface {}: {}",
                        iface_name, e
                    );
                }
                if let Err(e) = self.set_ipv6_info(&iface_name, ipv6).await {
                    warn!(
                        "Failed to apply ipv6 config for iface {}: {}",
                        iface_name, e
                    );
                }
            }
        }

        Ok(())
    }

    /// 事件循环：消费 DHCP 事件、网卡状态变化与外部命令。
    pub(super) async fn run(mut self) {
        // 启动网卡状态监听任务
        if let Err(e) = self.link_monitor.start() {
            error!("Failed to start link monitor: {}", e);
        }

        info!("network event loop started");

        loop {
            tokio::select! {
                // 处理外部命令
                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_cmd(cmd).await;
                }

                // 处理 DHCP 客户端上报的租约
                Some(lease) = self.dhcp_rx.recv() => {
                    if let Err(e) = self.handle_dhcp_lease(lease).await {
                        error!("Failed to handle DHCP lease: {}", e);
                    }
                }

                // 处理网卡状态变化
                Some(ev) = self.link_rx.recv() => {
                    if let Err(e) = self.handle_link_event(ev).await {
                        error!("Failed to handle link event: {}", e);
                    }
                }

                // 所有发送端均已释放，退出
                else => {
                    info!("all senders dropped, network event loop exiting");
                    break;
                }
            }
        }

        info!("network event loop exited");
    }

    /// 分发并执行外部命令，通过 oneshot 回传结果
    async fn handle_cmd(&mut self, cmd: Command) {
        match cmd {
            Command::SetIpv4 {
                iface_name,
                info,
                reply,
            } => {
                let res = self.set_ipv4_info(&iface_name, info).await;
                let _ = reply.send(res);
            }
            Command::SetIpv6 {
                iface_name,
                info,
                reply,
            } => {
                let res = self.set_ipv6_info(&iface_name, info).await;
                let _ = reply.send(res);
            }
            Command::GetNetCard { iface_name, reply } => {
                let res = self.get_netcard_info(&iface_name);
                let _ = reply.send(res);
            }
            Command::GetAllNetCards { reply } => {
                let res = self.get_all_netcards();
                let _ = reply.send(res);
            }
            Command::SetLinkState {
                iface_name,
                up,
                reply,
            } => {
                let res = self.set_link_state(&iface_name, up).await;
                let _ = reply.send(res);
            }
        }
    }

    /// 获取指定网卡的信息
    fn get_netcard_info(&self, iface_name: &str) -> Result<NetCardInfo, anyhow::Error> {
        self.netcards
            .get(iface_name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Interface '{}' not found", iface_name))
    }

    /// 获取所有网卡的信息
    fn get_all_netcards(&self) -> Result<Vec<NetCardInfo>, anyhow::Error> {
        let mut netcards: Vec<NetCardInfo> = self.netcards.values().cloned().collect();
        // 按网卡名称排序，保证返回顺序稳定
        netcards.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(netcards)
    }

    /// 启用/禁用网口（up/down），同步缓存并持久化开关状态。
    async fn set_link_state(&mut self, iface_name: &str, up: bool) -> Result<(), anyhow::Error> {
        // 校验网卡是否存在
        if !self.netcards.contains_key(iface_name) {
            return Err(anyhow::anyhow!("Interface '{}' not found", iface_name));
        }

        if up {
            self.netlink.set_link_up(iface_name).await?;
        } else {
            self.netlink.set_link_down(iface_name).await?;
        }

        // 同步缓存中的网卡开关状态
        if let Some(netcard) = self.netcards.get_mut(iface_name) {
            netcard.enabled = up;
        }

        // 持久化（仅 link 开关字段更新，IPv4/IPv6 配置保持原值）
        self.persist_config(iface_name);

        Ok(())
    }

    /// 将某张网卡当前的缓存配置（link 开关 + IPv4 + IPv6）持久化到磁盘。
    fn persist_config(&self, iface_name: &str) {
        if let Some(netcard) = self.netcards.get(iface_name)
            && let Err(e) =
                persist::save_full(iface_name, netcard.enabled, &netcard.ipv4, &netcard.ipv6)
        {
            warn!("Failed to persist config for iface {}: {}", iface_name, e);
        }
    }

    /// 设置 IPv4 信息
    async fn set_ipv4_info(
        &mut self,
        iface_name: &str,
        ipv4_info: Ipv4Info,
    ) -> Result<(), anyhow::Error> {
        // 1. 查缓存，取出旧的 IPv4 信息
        let old_ipv4 = if let Some(netcard) = self.netcards.get(iface_name) {
            netcard.ipv4.clone()
        } else {
            // 网卡不存在，直接返回错误
            return Err(anyhow::anyhow!("Interface '{}' not found", iface_name));
        };

        // 停止 DHCPv4 客户端（若有）
        if let Some(client) = self.dhcp4_clients.get_mut(iface_name) {
            client.stop().await?;
        }

        // 删除旧信息中已配置的静态 IP 和网关（DHCP 下发的由 stop() 释放）
        if !old_ipv4.ip.is_unspecified()
            && !old_ipv4.netmask.is_unspecified()
            && let Err(e) = self
                .netlink
                .del_ipv4_address(iface_name, old_ipv4.ip, old_ipv4.netmask)
                .await
        {
            warn!(
                "Failed to delete IPv4 address for iface {}: {}",
                iface_name, e
            );
        }
        if !old_ipv4.gateway.is_unspecified()
            && let Err(e) = self
                .netlink
                .del_ipv4_gateway(iface_name, old_ipv4.gateway)
                .await
        {
            warn!(
                "Failed to delete IPv4 gateway for iface {}: {}",
                iface_name, e
            );
        }

        // 2. 更新缓存（无论 enabled 是否为 true，开关状态都需要持久化）
        if let Some(netcard) = self.netcards.get_mut(iface_name) {
            netcard.ipv4 = ipv4_info.clone();
        }

        // enabled 为 false，仅持久化开关状态后返回
        if !ipv4_info.enabled {
            self.persist_config(iface_name);
            return Ok(());
        }

        // 3. 检查 use_dhcp 字段，如果为 false，配置静态 IP
        if !ipv4_info.use_dhcp {
            if !ipv4_info.ip.is_unspecified() && !ipv4_info.netmask.is_unspecified() {
                self.netlink
                    .set_ipv4_address(iface_name, ipv4_info.ip, ipv4_info.netmask)
                    .await?;
            }
            if !ipv4_info.gateway.is_unspecified() {
                self.netlink
                    .set_ipv4_gateway(iface_name, ipv4_info.gateway)
                    .await?;
            }
        }

        // 4. 检查 auto_dns 字段，如果为 false，配置静态 DNS
        if !ipv4_info.auto_dns {
            resolv::update_ipv4_dns(iface_name, ipv4_info.primary_dns, ipv4_info.secondary_dns)?;
        }

        // 5. 需要 DHCP 则start DHCP 客户端，否则持久化配置
        if ipv4_info.use_dhcp || ipv4_info.auto_dns {
            if let Some(client) = self.dhcp4_clients.get_mut(iface_name) {
                client.start().await?;
            }
        } else {
            self.persist_config(iface_name);
        }

        Ok(())
    }

    /// 设置 IPv6 信息
    async fn set_ipv6_info(
        &mut self,
        iface_name: &str,
        ipv6_info: Ipv6Info,
    ) -> Result<(), anyhow::Error> {
        // 1. 查缓存，取出旧的 IPv6 信息
        let old_ipv6 = if let Some(netcard) = self.netcards.get(iface_name) {
            netcard.ipv6.clone()
        } else {
            // 网卡不存在，直接返回错误
            return Err(anyhow::anyhow!("Interface '{}' not found", iface_name));
        };

        // 停止 DHCPv6 客户端（若有）
        if let Some(client) = self.dhcp6_clients.get_mut(iface_name) {
            client.stop().await?;
        }

        // 删除旧信息中已配置的静态 IP 和网关（DHCP 下发的由 stop() 释放）
        if !old_ipv6.ip.is_unspecified()
            && old_ipv6.prefix_len > 0
            && let Err(e) = self
                .netlink
                .del_ipv6_address(iface_name, old_ipv6.ip, old_ipv6.prefix_len)
                .await
        {
            warn!(
                "Failed to delete IPv6 address for iface {}: {}",
                iface_name, e
            );
        }

        if !old_ipv6.gateway.is_unspecified()
            && let Err(e) = self
                .netlink
                .del_ipv6_gateway(iface_name, old_ipv6.gateway)
                .await
        {
            warn!(
                "Failed to delete IPv6 gateway for iface {}: {}",
                iface_name, e
            );
        }

        // 2. 更新缓存（无论 enabled 是否为 true，开关状态都需要持久化）
        if let Some(netcard) = self.netcards.get_mut(iface_name) {
            netcard.ipv6 = ipv6_info.clone();
        }

        // enabled 为 false，仅持久化开关状态后返回
        if !ipv6_info.enabled {
            self.persist_config(iface_name);
            return Ok(());
        }

        // 3. 检查 use_dhcp 字段，如果为 false，配置静态 IP
        if !ipv6_info.use_dhcp {
            if !ipv6_info.ip.is_unspecified() && ipv6_info.prefix_len > 0 {
                self.netlink
                    .set_ipv6_address(iface_name, ipv6_info.ip, ipv6_info.prefix_len)
                    .await?;
            }
            if !ipv6_info.gateway.is_unspecified() {
                self.netlink
                    .set_ipv6_gateway(iface_name, ipv6_info.gateway)
                    .await?;
            }
        }

        // 4. 检查 auto_dns 字段，如果为 false，配置静态 DNS
        if !ipv6_info.auto_dns {
            resolv::update_ipv6_dns(iface_name, ipv6_info.primary_dns, ipv6_info.secondary_dns)?;
        }

        // 5. 需要 DHCPv6 则start DHCPv6 客户端，否则持久化配置
        if ipv6_info.use_dhcp || ipv6_info.auto_dns {
            if let Some(client) = self.dhcp6_clients.get_mut(iface_name) {
                client.start().await?;
            }
        } else {
            self.persist_config(iface_name);
        }

        Ok(())
    }
    /// 处理 DHCP 事件：根据 use_dhcp / auto_dns 应用 IP 与 DNS，并回写缓存
    async fn handle_dhcp_lease(&mut self, lease: DhcpEvent) -> Result<(), anyhow::Error> {
        match lease {
            DhcpEvent::V4(iface_name, lease) => self.handle_dhcp4_lease(&iface_name, lease).await,
            DhcpEvent::V6(iface_name, lease) => self.handle_dhcp6_lease(&iface_name, lease).await,
        }
    }

    /// 处理 DHCPv4 租约
    async fn handle_dhcp4_lease(
        &mut self,
        iface_name: &str,
        lease: Ipv4Info,
    ) -> Result<(), anyhow::Error> {
        let netcard = self
            .netcards
            .get_mut(iface_name)
            .ok_or_else(|| anyhow::anyhow!("Interface {} not found in cache", iface_name))?;

        if netcard.ipv4.use_dhcp {
            // 设置地址（DHCP 租约，直接应用）
            if !lease.ip.is_unspecified() {
                self.netlink
                    .set_ipv4_address(iface_name, lease.ip, lease.netmask)
                    .await?;
            }
            // 设置网关
            if !lease.gateway.is_unspecified() {
                self.netlink
                    .set_ipv4_gateway(iface_name, lease.gateway)
                    .await?;
            }
            // 回写地址/网关缓存
            netcard.ipv4.ip = lease.ip;
            netcard.ipv4.netmask = lease.netmask;
            netcard.ipv4.gateway = lease.gateway;
        }

        // 处理 DNS（仅 auto_dns）
        if netcard.ipv4.auto_dns {
            resolv::update_ipv4_dns(iface_name, lease.primary_dns, lease.secondary_dns)?;
            // 回写 DNS 缓存
            netcard.ipv4.primary_dns = lease.primary_dns;
            netcard.ipv4.secondary_dns = lease.secondary_dns;
        }

        self.persist_config(iface_name);

        // 租约更新后上报 Changed 事件
        if let Some(n) = self.netcards.get(iface_name).cloned() {
            self.emit_event(NetCardEvent::Changed(n));
        }

        Ok(())
    }

    /// 处理 DHCPv6 租约
    async fn handle_dhcp6_lease(
        &mut self,
        iface_name: &str,
        lease: Ipv6Info,
    ) -> Result<(), anyhow::Error> {
        let netcard = self
            .netcards
            .get_mut(iface_name)
            .ok_or_else(|| anyhow::anyhow!("Interface {} not found in cache", iface_name))?;

        if netcard.ipv6.use_dhcp {
            // 设置地址（DHCPv6 租约，直接应用）
            if !lease.ip.is_unspecified() && lease.prefix_len > 0 {
                self.netlink
                    .set_ipv6_address(iface_name, lease.ip, lease.prefix_len)
                    .await?;
            }
            // 设置网关（DHCPv6 通常由 RA 提供网关，此处仅在有值时设置）
            if !lease.gateway.is_unspecified() {
                self.netlink
                    .set_ipv6_gateway(iface_name, lease.gateway)
                    .await?;
            }
            // 回写地址/网关缓存
            netcard.ipv6.ip = lease.ip;
            netcard.ipv6.prefix_len = lease.prefix_len;
            netcard.ipv6.gateway = lease.gateway;
        }

        // 处理 DNS（仅 auto_dns）
        if netcard.ipv6.auto_dns {
            resolv::update_ipv6_dns(iface_name, lease.primary_dns, lease.secondary_dns)?;
            netcard.ipv6.primary_dns = lease.primary_dns;
            netcard.ipv6.secondary_dns = lease.secondary_dns;
        }

        self.persist_config(iface_name);

        // 租约更新后上报 Changed 事件
        if let Some(n) = self.netcards.get(iface_name).cloned() {
            self.emit_event(NetCardEvent::Changed(n));
        }

        Ok(())
    }

    /// 上报网卡变更事件（非阻塞，通道满时丢弃并告警）。
    fn emit_event(&self, ev: NetCardEvent) {
        if let Err(e) = self.event_tx.try_send(ev) {
            warn!("Failed to emit netcard event: {}", e);
        }
    }

    /// 处理网卡状态变化：新增时创建网卡，状态变化时刷新硬件信息，删除时清理。
    async fn handle_link_event(&mut self, ev: LinkEvent) -> Result<(), anyhow::Error> {
        if ev.added {
            if self.netcards.contains_key(&ev.iface_name) {
                // 已存在网卡的状态变化（up/down/插拔网线）：仅刷新硬件信息
                self.update_netcard_hardware(&ev.iface_name).await?;
                // 链路状态变化：上报 Changed 事件
                if let Some(n) = self.netcards.get(&ev.iface_name).cloned() {
                    self.emit_event(NetCardEvent::Changed(n));
                }
            } else {
                // 新增网卡：创建 NetCardInfo 与 DHCP 客户端
                info!("iface {} added", ev.iface_name);
                self.add_netcard(ev.iface_name.clone()).await?;
                // 上报 Added 事件
                if let Some(n) = self.netcards.get(&ev.iface_name).cloned() {
                    self.emit_event(NetCardEvent::Added(n));
                }
            }
        } else {
            // 网卡被删除：清理缓存与对应的 DHCP 客户端
            info!("iface {} removed", ev.iface_name);
            let removed = self.netcards.remove(&ev.iface_name);
            if let Some(mut c) = self.dhcp4_clients.remove(&ev.iface_name) {
                c.stop().await?;
            }
            if let Some(mut c) = self.dhcp6_clients.remove(&ev.iface_name) {
                c.stop().await?;
            }
            // 上报 Removed 事件
            if let Some(n) = removed {
                self.emit_event(NetCardEvent::Removed(n));
            }
        }

        Ok(())
    }

    /// 更新已存在网卡的硬件信息（如插拔网线导致的 LinkState 变化）。
    ///
    /// 当链路由“非 Up”变为 `Up`（网线插入 / 链路恢复）且网卡已启用时，
    /// 会从缓存读取 IPv4/IPv6 配置并重新下发，确保链路恢复后网络立即可用。
    async fn update_netcard_hardware(&mut self, iface_name: &str) -> Result<(), anyhow::Error> {
        let hardware = self.netlink.get_hardware_info(iface_name).await?;

        // 记录旧的链路状态，用于判断是否“变为 Up”
        let old_state = self
            .netcards
            .get(iface_name)
            .map(|n| n.hardware.state.clone());
        let new_state = hardware.state.clone();

        if let Some(netcard) = self.netcards.get_mut(iface_name) {
            info!(
                "iface {} state: {:?} -> {:?}",
                iface_name, netcard.hardware.state, hardware.state
            );
            netcard.hardware = hardware;
        }

        // 链路由“非 Up”恢复为 Up，且网卡已启用：从缓存重新下发 IP 配置
        let became_up = old_state.as_ref() != Some(&LinkState::Up) && new_state == LinkState::Up;
        if became_up {
            let (enabled, ipv4, ipv6) = match self.netcards.get(iface_name) {
                Some(n) => (n.enabled, n.ipv4.clone(), n.ipv6.clone()),
                None => return Ok(()),
            };
            if enabled {
                if let Err(e) = self.set_ipv4_info(iface_name, ipv4).await {
                    warn!(
                        "Failed to reapply ipv4 config for iface {}: {}",
                        iface_name, e
                    );
                }
                if let Err(e) = self.set_ipv6_info(iface_name, ipv6).await {
                    warn!(
                        "Failed to reapply ipv6 config for iface {}: {}",
                        iface_name, e
                    );
                }
            }
        }

        Ok(())
    }
}
