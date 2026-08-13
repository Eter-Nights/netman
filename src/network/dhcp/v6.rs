use super::super::DhcpEvent;
use crate::Ipv6Info;

use anyhow::Result;
use mozim::{DhcpV6Client, DhcpV6Config, DhcpV6Lease, DhcpV6Mode, DhcpV6OptionCode, DhcpV6State};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

pub struct Dhcp6Client {
    iface_name: String,
    client: Arc<Mutex<DhcpV6Client>>,
    task_handle: Option<JoinHandle<()>>,
    stop_tx: Option<mpsc::Sender<()>>,
    lease: Arc<Mutex<Option<DhcpV6Lease>>>, // 仅跟踪租约用于 stop() 时释放
    lease_tx: mpsc::Sender<DhcpEvent>,
}

impl Dhcp6Client {
    /// 创建新的 DHCPv6 客户端（异步构造器模式）
    pub async fn new(iface_name: String, lease_tx: mpsc::Sender<DhcpEvent>) -> Result<Self> {
        let config = DhcpV6Config::new(&iface_name, DhcpV6Mode::NonTemporaryAddresses);
        let client = DhcpV6Client::init(config, None).await?;

        Ok(Self {
            iface_name,
            client: Arc::new(Mutex::new(client)),
            task_handle: None,
            stop_tx: None,
            lease: Arc::new(Mutex::new(None)),
            lease_tx,
        })
    }

    /// 启动 DHCPv6 客户端循环
    pub async fn start(&mut self) -> Result<()> {
        if self.task_handle.is_some() {
            warn!("DHCPv6 client already started");
            return Ok(());
        }

        let (stop_tx, mut stop_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);

        let client = self.client.clone();
        let lease_tx = self.lease_tx.clone();
        let iface_name = self.iface_name.clone();
        let lease = self.lease.clone();

        let handle = tokio::spawn(async move {
            info!("DHCPv6 client started on interface: {}", iface_name);

            loop {
                tokio::select! {
                    _ = stop_rx.recv() => {
                        info!("DHCPv6 client received stop signal");
                        break;
                    }
                    result = async {
                        let mut client = client.lock().await;
                        client.run().await
                    } => {
                        match result {
                            Ok(state) => {
                                debug!("DHCPv6 state changed: {:?}", state);

                                if let DhcpV6State::Done(l) = &state {
                                    let lease_clone = (**l).clone();
                                    *lease.lock().await = Some(lease_clone.clone());
                                    info!("DHCPv6 lease acquired: {}", lease_clone.address);
                                    // 解析为 Ipv6Info 后发送
                                    let info = parse_lease(&lease_clone);
                                    if let Err(e) = lease_tx
                                        .send(DhcpEvent::V6(iface_name.clone(), info))
                                        .await
                                    {
                                        error!("Failed to send DHCPv6 lease: {}", e);
                                    }
                                }
                            }
                            Err(e) => error!("DHCPv6 error: {}", e),
                        }
                    }
                }
            }
            info!("DHCPv6 client loop exited");
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    /// 停止 DHCPv6 客户端并释放租约
    pub async fn stop(&mut self) -> Result<()> {
        // 1. 发送停止信号
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(()).await;
        }

        // 2. 等待后台任务退出
        if let Some(handle) = self.task_handle.take() {
            let _ = handle.await;
        }

        // 3. 释放租约（若有）
        if let Some(lease) = self.lease.lock().await.take() {
            info!("Releasing DHCPv6 lease: {}", lease.address);
            let mut client = self.client.lock().await;
            if let Err(e) = client.release(&lease).await {
                error!("Failed to release DHCPv6 lease: {}", e);
            }
            client.clean_up();
        } else {
            self.client.lock().await.clean_up();
        }

        info!("DHCPv6 client stopped");
        Ok(())
    }
}

impl Drop for Dhcp6Client {
    fn drop(&mut self) {
        // 仅发送停止信号，不等待、不释放租约（需显式调用 stop()）
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.try_send(());
        }
    }
}

/// 从 DHCPv6 lease 中解析主/备 DNS（OPTION_DNS_SERVERS = 23）
fn parse_v6_dns(lease: &DhcpV6Lease) -> (std::net::Ipv6Addr, std::net::Ipv6Addr) {
    let mut primary = std::net::Ipv6Addr::UNSPECIFIED;
    let mut secondary = std::net::Ipv6Addr::UNSPECIFIED;

    if let Some(raw_list) = lease.get_option_raw(u16::from(DhcpV6OptionCode::DnsServers)) {
        let mut filled = false;
        'outer: for raw in raw_list.iter() {
            for chunk in raw.chunks(16) {
                if chunk.len() != 16 {
                    continue;
                }
                let mut octets = [0u8; 16];
                octets.copy_from_slice(chunk);
                let addr = std::net::Ipv6Addr::from(octets);
                if !filled {
                    primary = addr;
                    filled = true;
                } else if secondary.is_unspecified() {
                    secondary = addr;
                    break 'outer;
                }
            }
        }
    }

    (primary, secondary)
}

/// 将 mozim 的 DhcpV6Lease 解析为 Ipv6Info
fn parse_lease(lease: &DhcpV6Lease) -> Ipv6Info {
    let (primary_dns, secondary_dns) = parse_v6_dns(lease);
    Ipv6Info {
        ip: lease.address,
        prefix_len: lease.prefix_len,
        gateway: std::net::Ipv6Addr::UNSPECIFIED,
        primary_dns,
        secondary_dns,
        ..Default::default()
    }
}
