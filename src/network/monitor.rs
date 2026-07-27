use anyhow::Result;
use futures::StreamExt;
use rtnetlink::{
    MulticastGroup, new_multicast_connection,
    packet_core::{NetlinkMessage, NetlinkPayload},
    packet_route::RouteNetlinkMessage,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// 网卡状态变化事件
#[derive(Clone, Debug)]
pub struct LinkEvent {
    pub iface_name: String,
    /// true = 网卡新增/状态变化（RTM_NEWLINK），false = 网卡被删除（RTM_DELLINK）
    pub added: bool,
}

/// 网卡状态监听器：订阅 netlink 多播（仅 Link 组），通过 channel 上报事件。
pub struct LinkMonitor {
    /// 对外上报通道发送端（Core 持有一份以防后台任务 clone drop 后通道过早关闭）
    event_tx: mpsc::Sender<LinkEvent>,
    stop_tx: Option<mpsc::Sender<()>>,
    task_handle: Option<JoinHandle<()>>,
}

impl LinkMonitor {
    /// 创建监听器：接收由 Core 统一创建的 channel 发送端，不 spawn 后台任务。
    pub fn new(event_tx: mpsc::Sender<LinkEvent>) -> Self {
        Self {
            event_tx,
            stop_tx: None,
            task_handle: None,
        }
    }

    /// 启动后台监听任务：在 task 内建立多播连接，生命周期绑定到 task。
    pub fn start(&mut self) -> Result<()> {
        if self.task_handle.is_some() {
            warn!("link monitor already started");
            return Ok(());
        }

        let (stop_tx, mut stop_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);

        let event_tx = self.event_tx.clone();
        let handle = tokio::spawn(async move {
            // 只订阅 Link 组，过滤掉 IP/路由等多播噪声
            let (conn, _handle, mut rx) = match new_multicast_connection(&[MulticastGroup::Link]) {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to create multicast connection: {}", e);
                    return;
                }
            };
            // 连接的生命周期与 task 绑定，task 退出自动 drop
            tokio::spawn(conn);
            info!("link monitor started");

            loop {
                tokio::select! {
                    // 停止信号
                    _ = stop_rx.recv() => {
                        info!("link monitor received stop signal");
                        break;
                    }
                    // 接收内核多播消息
                    msg = rx.next() => {
                        match msg {
                            Some((nlmsg, _src)) => {
                                if let Some(ev) = parse_link_event(&nlmsg) {
                                    if let Err(e) = event_tx.send(ev).await {
                                        error!("Failed to send link event: {}", e);
                                        break;
                                    }
                                }
                            }
                            None => {
                                info!("netlink multicast stream closed");
                                break;
                            }
                        }
                    }
                }
            }
            info!("link monitor loop exited");
        });

        self.task_handle = Some(handle);
        Ok(())
    }
}

impl Drop for LinkMonitor {
    fn drop(&mut self) {
        // Drop 只能同步执行，无法 await：仅发停止信号，
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.try_send(());
        }
    }
}

/// 从 LinkMessage 的属性中提取网卡名称（IfName）。
fn link_name(link: &rtnetlink::packet_route::link::LinkMessage) -> Option<String> {
    use rtnetlink::packet_route::link::LinkAttribute;
    link.attributes.iter().find_map(|attr| match attr {
        LinkAttribute::IfName(n) => Some(n.clone()),
        _ => None,
    })
}

/// 解析单条 netlink 消息为 LinkEvent。
fn parse_link_event(msg: &NetlinkMessage<RouteNetlinkMessage>) -> Option<LinkEvent> {
    if let NetlinkPayload::InnerMessage(route_msg) = &msg.payload {
        match route_msg {
            RouteNetlinkMessage::NewLink(link) => link_name(link).map(|iface_name| LinkEvent {
                iface_name,
                added: true,
            }),
            RouteNetlinkMessage::DelLink(link) => link_name(link).map(|iface_name| LinkEvent {
                iface_name,
                added: false,
            }),
            _ => None,
        }
    } else {
        None
    }
}
