//! 基于 Unix domain socket 的 IPC 层。
//!
//! 协议：每行一条 JSON（换行分隔）。请求使用 JSON-RPC 2.0 风格，
//! 通过 `id` 匹配响应；事件为单向推送（无 `id`）。

use crate::{Ipv4Info, Ipv6Info, NetworkHandle};

use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc};

/// 默认 socket 路径
pub const DEFAULT_SOCKET_PATH: &str = "/tmp/netman.sock";

/// 客户端请求
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "method", content = "params", rename_all = "snake_case")]
pub enum Request {
    GetNetCard {
        iface_name: String,
    },
    GetAllNetCards,
    SetLinkState {
        iface_name: String,
        up: bool,
    },
    SetIpv4 {
        iface_name: String,
        info: Ipv4Info,
    },
    SetIpv6 {
        iface_name: String,
        info: Ipv6Info,
    },
    /// 订阅网卡事件推送（服务端持续单向推送事件，无后续响应）
    Subscribe,
}

/// 客户端带 `id` 的请求包装（用于匹配响应）
#[derive(Serialize, Deserialize, Debug)]
pub struct Envelope {
    pub id: u64,
    #[serde(flatten)]
    pub request: Request,
}

/// RPC 错误
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

impl RpcError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// 服务端响应
#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl Response {
    pub fn ok(id: u64, result: serde_json::Value) -> Self {
        Self {
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(id: u64, code: i32, message: impl Into<String>) -> Self {
        Self {
            id,
            result: None,
            error: Some(RpcError::new(code, message)),
        }
    }
}

/// 启动 IPC 服务，返回后已进入 accept 循环。
///
/// 若 `path` 已存在会被删除；进程退出时由调用者负责清理。
pub async fn serve(handle: NetworkHandle, path: impl AsRef<Path>) -> std::io::Result<()> {
    let path = path.as_ref();

    // 清理可能残留的旧 socket 文件
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }

    let listener = UnixListener::bind(path)?;
    tracing::info!("IPC listening on {}", path.display());

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let h = handle.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(h, stream).await {
                        tracing::warn!("client session ended: {e}");
                    }
                });
            }
            Err(e) => tracing::warn!("accept failed: {e}"),
        }
    }
}

/// 处理单个客户端连接。
///
/// 读循环按行解析请求；所有出站数据（响应与事件）都经一个共享的
/// `mpsc` 通道汇入写任务，从而避免响应与事件并发写入时的交错。
async fn handle_client(handle: NetworkHandle, stream: UnixStream) -> std::io::Result<()> {
    let (read_half, write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(32);
    let mut subscriber_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // 写任务：把出站字符串逐行写回 socket
    let writer = tokio::spawn(async move {
        let mut w = write_half;
        while let Some(mut line) = out_rx.recv().await {
            line.push('\n');
            if w.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    while let Ok(Some(line)) = lines.next_line().await {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let Envelope { id, request } = match serde_json::from_str::<Envelope>(line) {
            Ok(env) => env,
            Err(e) => {
                let resp = Response::err(0, -32700, format!("parse error: {e}"));
                if let Ok(s) = serde_json::to_string(&resp) {
                    let _ = out_tx.send(s).await;
                }
                continue;
            }
        };

        match request {
            Request::Subscribe => {
                // 先回复 ack，告知订阅已建立
                let ack = Response::ok(id, serde_json::json!("subscribed"));
                if let Ok(s) = serde_json::to_string(&ack) {
                    let _ = out_tx.send(s).await;
                }
                // 启动事件转发任务，持续推送直到连接断开
                let mut rx = handle.subscribe();
                let out_tx_ev = out_tx.clone();
                let sub = tokio::spawn(async move {
                    loop {
                        match rx.recv().await {
                            Ok(ev) => {
                                let Ok(s) = serde_json::to_string(&ev) else {
                                    continue;
                                };
                                if out_tx_ev.send(s).await.is_err() {
                                    break; // 客户端断开
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!("event subscriber lagged by {n} messages");
                                continue;
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                });
                subscriber_tasks.push(sub);
            }
            other => {
                let h = handle.clone();
                let out_tx_req = out_tx.clone();
                tokio::spawn(async move {
                    let resp = dispatch(&h, Envelope { id, request: other }).await;
                    if let Ok(s) = serde_json::to_string(&resp) {
                        let _ = out_tx_req.send(s).await;
                    }
                });
            }
        }
    }

    // 客户端断开：停止事件订阅任务，再关闭出站通道让写任务退出
    for t in subscriber_tasks {
        t.abort();
    }
    drop(out_tx);
    let _ = writer.await;
    Ok(())
}

/// 把请求分发到 `NetworkHandle`，返回响应。
async fn dispatch(handle: &NetworkHandle, env: Envelope) -> Response {
    let id = env.id;
    let result: anyhow::Result<serde_json::Value> = match env.request {
        Request::GetNetCard { iface_name } => handle
            .get_netcard_info(&iface_name)
            .await
            .and_then(|c| serde_json::to_value(c).map_err(Into::into)),
        Request::GetAllNetCards => handle
            .get_all_netcards()
            .await
            .and_then(|cs| serde_json::to_value(cs).map_err(Into::into)),
        Request::SetLinkState { iface_name, up } => handle
            .set_link_state(&iface_name, up)
            .await
            .map(|()| serde_json::Value::Null),
        Request::SetIpv4 { iface_name, info } => handle
            .set_ipv4_info(&iface_name, info)
            .await
            .map(|()| serde_json::Value::Null),
        Request::SetIpv6 { iface_name, info } => handle
            .set_ipv6_info(&iface_name, info)
            .await
            .map(|()| serde_json::Value::Null),
        // Subscribe 在连接层处理，正常不会进入 dispatch
        Request::Subscribe => Ok(serde_json::Value::Null),
    };

    match result {
        Ok(v) => Response::ok(id, v),
        Err(e) => Response::err(id, -32000, format!("{e:#}")),
    }
}

/// 同步客户端：连接到指定 socket，发送一个请求并读取一行响应。
///
/// 供 CLI 工具使用；不需要异步运行时以外的复杂状态。
///
/// 注意：服务端在写回响应后不会关闭连接（仍保留读循环以支持长连接），
/// 因此这里只读取**第一行**即返回，不能使用 `read_to_end`，否则会因等
/// 待 EOF 而永久阻塞。
pub fn call(socket: impl AsRef<Path>, env: &Envelope) -> std::io::Result<Response> {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixStream as StdStream;

    let s = StdStream::connect(socket.as_ref())?;
    let mut writer = s.try_clone()?;
    let mut req = serde_json::to_string(env)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    req.push('\n');
    writer.write_all(req.as_bytes())?;
    writer.flush()?;

    // 只读取一行响应即返回（服务端保持连接打开以复用长连接）
    let mut reader = BufReader::new(s);
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "no response",
        ));
    }

    serde_json::from_str(line.trim())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
