use crate::Ipv4Info;
use crate::network::DhcpEvent;

use anyhow::Result;
use mozim::{DhcpV4Client, DhcpV4Config, DhcpV4Lease, DhcpV4State};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

pub struct Dhcp4Client {
    iface_name: String,
    client: Arc<Mutex<DhcpV4Client>>,
    task_handle: Option<JoinHandle<()>>,
    stop_tx: Option<mpsc::Sender<()>>,
    lease: Arc<Mutex<Option<DhcpV4Lease>>>,
    lease_tx: mpsc::Sender<DhcpEvent>,
}

impl Dhcp4Client {
    /// 创建新的 DHCPv4 客户端（异步构造器模式）
    pub async fn new(iface_name: String, lease_tx: mpsc::Sender<DhcpEvent>) -> Result<Self> {
        let config = DhcpV4Config::new(&iface_name);
        let client = DhcpV4Client::init(config, None).await?;

        Ok(Self {
            iface_name,
            client: Arc::new(Mutex::new(client)),
            task_handle: None,
            stop_tx: None,
            lease: Arc::new(Mutex::new(None)),
            lease_tx,
        })
    }

    /// 启动 DHCP 客户端循环
    pub async fn start(&mut self) -> Result<()> {
        if self.task_handle.is_some() {
            warn!("DHCP client already started");
            return Ok(());
        }

        let (stop_tx, mut stop_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);

        let client = self.client.clone();
        let lease_tx = self.lease_tx.clone();
        let iface_name = self.iface_name.clone();
        let lease = self.lease.clone();

        let handle = tokio::spawn(async move {
            info!("DHCP client started on interface: {}", iface_name);

            loop {
                tokio::select! {
                    _ = stop_rx.recv() => {
                        info!("DHCP client received stop signal");
                        break;
                    }
                    result = async {
                        let mut client = client.lock().await;
                        client.run().await
                    } => {
                        match result {
                            Ok(state) => {
                                debug!("DHCP state changed: {:?}", state);

                                if let DhcpV4State::Done(l) = &state {
                                    let lease_clone = (**l).clone();
                                    *lease.lock().await = Some(lease_clone.clone());
                                    info!("DHCP lease acquired: {}", lease_clone.yiaddr);
                                    // 解析为 Ipv4Info 后发送
                                    let info = parse_lease(&lease_clone);
                                    if let Err(e) = lease_tx
                                        .send(DhcpEvent::V4(iface_name.clone(), info))
                                        .await
                                    {
                                        error!("Failed to send DHCP lease: {}", e);
                                    }
                                }
                            }
                            Err(e) => error!("DHCP error: {}", e),
                        }
                    }
                }
            }
            info!("DHCP client loop exited");
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    /// 停止 DHCP 客户端并释放租约
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
            info!("Releasing DHCP lease: {}", lease.yiaddr);
            let mut client = self.client.lock().await;
            if let Err(e) = client.release(&lease).await {
                error!("Failed to release DHCP lease: {}", e);
            }
            client.clean_up();
        } else {
            self.client.lock().await.clean_up();
        }

        info!("DHCP client stopped");
        Ok(())
    }
}

impl Drop for Dhcp4Client {
    fn drop(&mut self) {
        // 仅发送停止信号，不等待、不释放租约（需显式调用 stop()）
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.try_send(());
        }
    }
}

/// 将 mozim 的 DhcpV4Lease 解析为 Ipv4Info
fn parse_lease(lease: &DhcpV4Lease) -> Ipv4Info {
    Ipv4Info {
        ip: lease.yiaddr,
        netmask: lease.subnet_mask,
        gateway: lease
            .gateways
            .as_ref()
            .and_then(|gws| gws.first().copied())
            .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED),
        primary_dns: lease
            .dns_srvs
            .as_ref()
            .and_then(|dns| dns.first().copied())
            .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED),
        secondary_dns: lease
            .dns_srvs
            .as_ref()
            .and_then(|dns| dns.get(1).copied())
            .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED),
        ..Default::default()
    }
}
