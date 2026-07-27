use netman::{NetworkHandle, DEFAULT_SOCKET_PATH};

use std::fs::File;

fn init_logging() {
    let file = File::options()
        .create(true)
        .append(true)
        .open("/var/log/netman.log")
        .expect("Failed to open log file");

    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_thread_names(false)
        .with_thread_ids(false)
        .with_target(false)
        .with_writer(file)
        .init();
}

fn main() {
    init_logging();
    tracing::info!("Starting device manager server...");

    let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    runtime.block_on(async {
        let handle = NetworkHandle::new().await;
        tracing::info!("network manager started");

        // 启动 IPC 服务；退出时清理 socket 文件
        let socket_path = DEFAULT_SOCKET_PATH.to_string();
        let ipc_handle = tokio::spawn(netman::serve(handle, socket_path.clone()));

        // 等待 Ctrl+C
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");
        tracing::info!("received Ctrl+C, shutting down");

        ipc_handle.abort();
        let _ = std::fs::remove_file(&socket_path);
        tracing::info!("cleaned up socket {}", socket_path);
    });
}
