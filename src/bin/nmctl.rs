//! nmctl —— netman 的命令行客户端。
//!
//! 通过 Unix domain socket 与后台守护进程通讯。
//!
//! 用法示例：
//!     nmctl list
//!     nmctl info eth0
//!     nmctl link eth0 up
//!     nmctl link eth0 down
//!     nmctl ipv4 eth0 --dhcp
//!     nmctl ipv4 eth0 --ip 192.168.1.10 --netmask 255.255.255.0 --gw 192.168.1.1

use netman::{call, NetCardEvent, DEFAULT_SOCKET_PATH, Envelope, Request, Response, RpcError};

use clap::{Parser, Subcommand};
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(
    name = "nmctl",
    version,
    about = "Command-line client for the netman daemon"
)]
struct Cli {
    /// 守护进程的 Unix socket 路径
    #[arg(long, default_value = DEFAULT_SOCKET_PATH, global = true)]
    socket: PathBuf,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// 列出所有网卡
    List,
    /// 查看指定网卡的详细信息
    Info { iface: String },
    /// 启用/禁用网口链路
    Link {
        iface: String,
        /// up 或 down
        state: LinkStateArg,
    },
    /// 设置 IPv4（静态或 DHCP）
    Ipv4 {
        iface: String,
        /// 使用 DHCP 自动获取
        #[arg(long)]
        dhcp: bool,
        /// 静态 IP 地址（与 --dhcp 互斥）
        #[arg(long, requires = "netmask")]
        ip: Option<Ipv4Addr>,
        /// 子网掩码
        #[arg(long)]
        netmask: Option<Ipv4Addr>,
        /// 网关
        #[arg(long)]
        gw: Option<Ipv4Addr>,
        /// 主 DNS
        #[arg(long)]
        dns1: Option<Ipv4Addr>,
        /// 备用 DNS
        #[arg(long)]
        dns2: Option<Ipv4Addr>,
    },
    /// 订阅网卡事件（持续运行，Ctrl+C 退出）
    Watch,
}

#[derive(Clone, Debug)]
enum LinkStateArg {
    Up,
    Down,
}

impl std::str::FromStr for LinkStateArg {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "up" => Ok(Self::Up),
            "down" => Ok(Self::Down),
            other => Err(format!("invalid link state `{other}` (expected up|down)")),
        }
    }
}

fn next_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn rpc(socket: &std::path::Path, request: Request) -> std::io::Result<Response> {
    let env = Envelope {
        id: next_id(),
        request,
    };
    call(socket, &env)
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: &Cli) -> std::io::Result<()> {
    let socket = &cli.socket;
    if matches!(cli.cmd, Cmd::Watch) {
        return run_watch(socket);
    }
    let req = match &cli.cmd {
        Cmd::List => Request::GetAllNetCards,
        Cmd::Info { iface } => Request::GetNetCard {
            iface_name: iface.clone(),
        },
        Cmd::Link { iface, state } => Request::SetLinkState {
            iface_name: iface.clone(),
            up: matches!(state, LinkStateArg::Up),
        },
        Cmd::Ipv4 {
            iface,
            dhcp,
            ip,
            netmask,
            gw,
            dns1,
            dns2,
        } => {
            if *dhcp && ip.is_some() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--dhcp 与 --ip 互斥",
                ));
            }
            if !*dhcp && (ip.is_none() || netmask.is_none()) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "静态模式需要 --ip 与 --netmask",
                ));
            }
            let info = netman::Ipv4Info::new(
                true,  // enabled
                *dhcp, // use_dhcp
                *dhcp, // auto_dns
                ip.unwrap_or(Ipv4Addr::UNSPECIFIED),
                netmask.unwrap_or(Ipv4Addr::UNSPECIFIED),
                gw.unwrap_or(Ipv4Addr::UNSPECIFIED),
                dns1.unwrap_or(Ipv4Addr::UNSPECIFIED),
                dns2.unwrap_or(Ipv4Addr::UNSPECIFIED),
            );
            Request::SetIpv4 {
                iface_name: iface.clone(),
                info,
            }
        }
        // Watch 已在上方提前 return，此处不可达
        Cmd::Watch => unreachable!(),
    };

    let resp = rpc(socket, req)?;
    print_response(&cli.cmd, resp);
    Ok(())
}

fn run_watch(socket: &std::path::Path) -> std::io::Result<()> {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixStream;

    let mut s = UnixStream::connect(socket)?;
    let env = Envelope {
        id: next_id(),
        request: Request::Subscribe,
    };
    let mut req = serde_json::to_string(&env)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    req.push('\n');
    s.write_all(req.as_bytes())?;

    println!("已订阅网卡事件，等待推送（Ctrl+C 退出）…");

    // 持续读取：首条是订阅 ack（可解析为 Response），其后均为事件
    let reader = BufReader::new(s.try_clone()?);
    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("读取事件失败: {e}");
                break;
            }
        };
        if serde_json::from_str::<Response>(&line).is_ok() {
            // 订阅确认，跳过
            continue;
        }
        match serde_json::from_str::<NetCardEvent>(&line) {
            Ok(ev) => print_event(&ev),
            Err(e) => eprintln!("无法解析的行: {line} ({e})"),
        }
    }
    Ok(())
}

fn print_event(ev: &NetCardEvent) {
    let (tag, info) = match ev {
        NetCardEvent::Added(c) => ("ADD", c),
        NetCardEvent::Removed(c) => ("DEL", c),
        NetCardEvent::Changed(c) => ("CHG", c),
    };
    let link = format!("{:?}", info.hardware.state);
    println!(
        "[{tag}] {:<14} link={:<14} ipv4={}",
        info.name, link, info.ipv4.ip
    );
}

fn print_response(cmd: &Cmd, resp: Response) {
    match resp.error {
        Some(RpcError { code, message }) => {
            eprintln!("rpc error (code {code}): {message}");
            std::process::exit(1);
        }
        None => {
            let value = resp.result.unwrap_or(serde_json::Value::Null);
            match cmd {
                Cmd::List => print_json_list(&value),
                Cmd::Info { .. } => print_json_one(&value),
                Cmd::Link { .. } | Cmd::Ipv4 { .. } => println!("ok"),
                // Watch 在 run() 中已单独处理，不会走到这里
                Cmd::Watch => unreachable!(),
            }
        }
    }
}

fn print_json_list(value: &serde_json::Value) {
    match value {
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                println!("(no network cards)");
                return;
            }
            // 简洁表格：名称 / 状态 / IPv4
            println!("{:<14} {:<10} {}", "NAME", "LINK", "IPV4");
            for v in arr {
                let name = v.get("name").and_then(|n| n.as_str()).unwrap_or("?");
                let link = v
                    .pointer("/hardware/state")
                    .map(|s| s.as_str().unwrap_or("?"))
                    .unwrap_or("?");
                let ip = v
                    .pointer("/ipv4/ip")
                    .and_then(|s| s.as_str())
                    .unwrap_or("-");
                println!("{:<14} {:<10} {}", name, link, ip);
            }
        }
        other => println!("{other}"),
    }
}

fn print_json_one(value: &serde_json::Value) {
    match serde_json::to_string_pretty(value) {
        Ok(s) => println!("{s}"),
        Err(_) => println!("{value}"),
    }
}
