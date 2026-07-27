mod actor;
mod dhcp;
mod monitor;
mod netlink;
mod persist;
mod resolv;

use self::actor::NetworkActor;
use crate::{Ipv4Info, Ipv6Info, NetCardInfo};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};

/// 网络管理命令
pub(crate) enum Command {
    GetNetCard {
        iface_name: String,
        reply: oneshot::Sender<Result<NetCardInfo, anyhow::Error>>,
    },
    GetAllNetCards {
        reply: oneshot::Sender<Result<Vec<NetCardInfo>, anyhow::Error>>,
    },
    SetLinkState {
        iface_name: String,
        up: bool,
        reply: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    SetIpv4 {
        iface_name: String,
        info: Ipv4Info,
        reply: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    SetIpv6 {
        iface_name: String,
        info: Ipv6Info,
        reply: oneshot::Sender<Result<(), anyhow::Error>>,
    },
}

/// DHCP 租约事件
#[derive(Clone)]
pub(crate) enum DhcpEvent {
    V4(String, Ipv4Info), // iface_name, lease
    V6(String, Ipv6Info), // iface_name, lease
}

/// 网卡变更事件：由 actor 通过事件通道上报给外部订阅者。
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event", content = "params", rename_all = "snake_case")]
pub enum NetCardEvent {
    /// 新增网卡
    Added(NetCardInfo),
    /// 网卡被删除
    Removed(NetCardInfo),
    /// 网卡信息发生变化（链路状态 / IP 租约更新等）
    Changed(NetCardInfo),
}

/// 网络管理器的对外句柄，持有命令发送端与事件广播端。
#[derive(Clone)]
pub struct NetworkHandle {
    cmd_tx: mpsc::Sender<Command>,
    event_tx: broadcast::Sender<NetCardEvent>,
}

impl NetworkHandle {
    /// 创建网络管理句柄。
    pub async fn new() -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (actor_event_tx, mut actor_event_rx) = mpsc::channel(32);
        let (event_tx, _) = broadcast::channel::<NetCardEvent>(64);

        let actor = NetworkActor::new(cmd_rx, actor_event_tx).await;
        tokio::spawn(actor.run());

        // 转发 actor 上报的事件到广播通道，供 IPC 订阅者消费；同时记录日志
        let fwd = event_tx.clone();
        tokio::spawn(async move {
            while let Some(ev) = actor_event_rx.recv().await {
                tracing::info!("netcard event: {:?}", ev);
                // 没有订阅者时 send 返回错误，忽略即可
                let _ = fwd.send(ev);
            }
        });

        Self { cmd_tx, event_tx }
    }

    /// 订阅网卡变更事件。
    ///
    /// 每个订阅者获得独立的接收端；若消费过慢会丢失部分事件
    /// （`broadcast::Receiver::recv` 会返回 `Lagged`）。
    pub fn subscribe(&self) -> broadcast::Receiver<NetCardEvent> {
        self.event_tx.subscribe()
    }

    /// 获取指定网卡的信息
    pub async fn get_netcard_info(&self, iface_name: &str) -> Result<NetCardInfo, anyhow::Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::GetNetCard {
                iface_name: iface_name.to_string(),
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("event loop closed"))?;

        match reply_rx.await {
            Ok(res) => res,
            Err(_) => Err(anyhow::anyhow!("dropped reply")),
        }
    }

    /// 获取所有网卡的信息
    pub async fn get_all_netcards(&self) -> Result<Vec<NetCardInfo>, anyhow::Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::GetAllNetCards { reply: reply_tx })
            .await
            .map_err(|_| anyhow::anyhow!("event loop closed"))?;

        match reply_rx.await {
            Ok(res) => res,
            Err(_) => Err(anyhow::anyhow!("dropped reply")),
        }
    }

    /// 启用/禁用指定网口（true=up，false=down）
    pub async fn set_link_state(&self, iface_name: &str, up: bool) -> Result<(), anyhow::Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SetLinkState {
                iface_name: iface_name.to_string(),
                up,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("event loop closed"))?;

        match reply_rx.await {
            Ok(res) => res,
            Err(_) => Err(anyhow::anyhow!("dropped reply")),
        }
    }

    /// 设置指定网卡的 IPv4 信息
    pub async fn set_ipv4_info(
        &self,
        iface_name: &str,
        info: Ipv4Info,
    ) -> Result<(), anyhow::Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SetIpv4 {
                iface_name: iface_name.to_string(),
                info,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("event loop closed"))?;

        match reply_rx.await {
            Ok(res) => res,
            Err(_) => Err(anyhow::anyhow!("dropped reply")),
        }
    }

    /// 设置指定网卡的 IPv6 信息
    pub async fn set_ipv6_info(
        &self,
        iface_name: &str,
        info: Ipv6Info,
    ) -> Result<(), anyhow::Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SetIpv6 {
                iface_name: iface_name.to_string(),
                info,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("event loop closed"))?;

        match reply_rx.await {
            Ok(res) => res,
            Err(_) => Err(anyhow::anyhow!("dropped reply")),
        }
    }
}
