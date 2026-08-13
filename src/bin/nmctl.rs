//! nmctl —— netman 的交互式命令行客户端。
//!
//! 直接链接 `netman` 库操作网络，无需独立的守护进程或 socket。
//! 启动后进入 REPL：从 stdin 逐行读取命令执行，同时后台订阅网卡事件并即时打印。
//!
//! 可用命令（输入 `help` 查看帮助）：
//!     list | ls
//!     info <iface>
//!     link <iface> up|down
//!     ipv4 <iface> --dhcp | --ip <ip> --netmask <mask> [--gw <gw>] [--dns1 <d1>] [--dns2 <d2>]
//!     ipv6 <iface> --dhcp | --ip <ip> --prefix <n> [--gw <gw>] [--dns1 <d1>] [--dns2 <d2>]
//!     help | h
//!     quit | exit | q

use netman::{Ipv4Info, Ipv6Info, NetCardEvent, NetCardInfo, NetworkHandle};

use std::net::{Ipv4Addr, Ipv6Addr};
use std::process::ExitCode;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::broadcast;

/// REPL 中解析出的命令。
enum Command {
    List,
    Info {
        iface: String,
    },
    Link {
        iface: String,
        up: bool,
    },
    Ipv4 {
        iface: String,
        info: Ipv4Info,
    },
    Ipv6 {
        iface: String,
        info: Ipv6Info,
    },
    Help,
    Quit,
    /// 空行，无需处理。
    Noop,
}

fn init_logging() {
    // 日志输出到 stderr，stdout 留给命令结果与网卡事件
    tracing_subscriber::fmt()
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}

fn main() -> ExitCode {
    init_logging();

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: failed to create Tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    runtime.block_on(run())
}

async fn run() -> ExitCode {
    let handle = match NetworkHandle::new().await {
        Ok(h) => h,
        Err(e) => {
            eprintln!("error: failed to initialize network manager: {e:#}");
            return ExitCode::FAILURE;
        }
    };

    // 后台订阅任务：持续把网卡事件打印到 stdout
    let mut rx = handle.subscribe();
    let event_task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(ev) => print_event(&ev),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    eprintln!("[warn] 事件流落后，丢弃了 {n} 条事件");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // REPL 主循环：从 stdin 逐行读取命令
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    print_banner();
    loop {
        print!("netman> ");
        use std::io::Write;
        let _ = std::io::stdout().flush();

        let line = match lines.next_line().await {
            Ok(Some(l)) => l,
            Ok(None) => {
                // stdin EOF
                println!();
                break;
            }
            Err(e) => {
                eprintln!("error: 读取输入失败: {e}");
                break;
            }
        };

        match parse_line(&line) {
            Ok(Command::Noop) => {}
            Ok(Command::Quit) => break,
            Ok(Command::Help) => print_help(),
            Ok(cmd) => {
                if let Err(e) = execute(&handle, cmd).await {
                    eprintln!("error: {e:#}");
                }
            }
            Err(e) => eprintln!("error: {e}"),
        }
    }

    event_task.abort();
    ExitCode::SUCCESS
}

/// 把一行输入解析为命令。
fn parse_line(line: &str) -> Result<Command, String> {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    let Some(&cmd) = tokens.first() else {
        return Ok(Command::Noop);
    };

    match cmd {
        "list" | "ls" => Ok(Command::List),
        "help" | "h" => Ok(Command::Help),
        "quit" | "exit" | "q" => Ok(Command::Quit),
        "info" => {
            let iface = tokens.get(1).ok_or("用法: info <iface>")?;
            Ok(Command::Info {
                iface: iface.to_string(),
            })
        }
        "link" => {
            let iface = tokens.get(1).ok_or("用法: link <iface> up|down")?;
            let up = match tokens.get(2).copied() {
                Some("up") => true,
                Some("down") => false,
                Some(other) => {
                    return Err(format!("无效的链路状态 `{other}`（应为 up|down）"));
                }
                None => return Err("用法: link <iface> up|down".to_string()),
            };
            Ok(Command::Link {
                iface: iface.to_string(),
                up,
            })
        }
        "ipv4" => parse_ipv4(&tokens[1..]),
        "ipv6" => parse_ipv6(&tokens[1..]),
        other => Err(format!("未知命令 `{other}`（输入 help 查看帮助）")),
    }
}

/// 解析 IPv4 配置命令。
///
/// 用法：`ipv4 <iface> --dhcp` 或
/// `ipv4 <iface> --ip <ip> --netmask <mask> [--gw <gw>] [--dns1 <d1>] [--dns2 <d2>]`
fn parse_ipv4(args: &[&str]) -> Result<Command, String> {
    const USAGE: &str = "用法: ipv4 <iface> --dhcp | --ip <ip> --netmask <mask> [--gw <gw>] [--dns1 <d1>] [--dns2 <d2>]";

    let iface = args.first().ok_or(USAGE)?.to_string();

    let mut dhcp = false;
    let mut ip = None;
    let mut netmask = None;
    let mut gw = None;
    let mut dns1 = None;
    let mut dns2 = None;

    let mut i = 1;
    while i < args.len() {
        match args[i] {
            "--dhcp" => dhcp = true,
            "--ip" => {
                i += 1;
                ip = Some(parse_ipv4_addr(args.get(i))?);
            }
            "--netmask" => {
                i += 1;
                netmask = Some(parse_ipv4_addr(args.get(i))?);
            }
            "--gw" => {
                i += 1;
                gw = Some(parse_ipv4_addr(args.get(i))?);
            }
            "--dns1" => {
                i += 1;
                dns1 = Some(parse_ipv4_addr(args.get(i))?);
            }
            "--dns2" => {
                i += 1;
                dns2 = Some(parse_ipv4_addr(args.get(i))?);
            }
            other => return Err(format!("未知选项 `{other}`（{USAGE}）")),
        }
        i += 1;
    }

    if dhcp && ip.is_some() {
        return Err("--dhcp 与 --ip 互斥".to_string());
    }
    if !dhcp && (ip.is_none() || netmask.is_none()) {
        return Err("静态模式需要 --ip 与 --netmask".to_string());
    }

    let info = Ipv4Info {
        enabled: true,
        use_dhcp: dhcp,
        auto_dns: dhcp,
        ip: ip.unwrap_or(Ipv4Addr::UNSPECIFIED),
        netmask: netmask.unwrap_or(Ipv4Addr::UNSPECIFIED),
        gateway: gw.unwrap_or(Ipv4Addr::UNSPECIFIED),
        primary_dns: dns1.unwrap_or(Ipv4Addr::UNSPECIFIED),
        secondary_dns: dns2.unwrap_or(Ipv4Addr::UNSPECIFIED),
    };

    Ok(Command::Ipv4 { iface, info })
}

/// 解析 IPv6 配置命令。
///
/// 用法：`ipv6 <iface> --dhcp` 或
/// `ipv6 <iface> --ip <ip> --prefix <n> [--gw <gw>] [--dns1 <d1>] [--dns2 <d2>]`
fn parse_ipv6(args: &[&str]) -> Result<Command, String> {
    const USAGE: &str = "用法: ipv6 <iface> --dhcp | --ip <ip> --prefix <n> [--gw <gw>] [--dns1 <d1>] [--dns2 <d2>]";

    let iface = args.first().ok_or(USAGE)?.to_string();

    let mut dhcp = false;
    let mut ip = None;
    let mut prefix_len = None;
    let mut gw = None;
    let mut dns1 = None;
    let mut dns2 = None;

    let mut i = 1;
    while i < args.len() {
        match args[i] {
            "--dhcp" => dhcp = true,
            "--ip" => {
                i += 1;
                ip = Some(parse_ipv6_addr(args.get(i))?);
            }
            "--prefix" => {
                i += 1;
                let raw = args.get(i).ok_or("--prefix 需要数值参数")?;
                let n: u8 = raw
                    .parse()
                    .map_err(|_| format!("无效的前缀长度 `{raw}`（应为 0-128）"))?;
                if n > 128 {
                    return Err(format!("无效的前缀长度 `{n}`（应为 0-128）"));
                }
                prefix_len = Some(n);
            }
            "--gw" => {
                i += 1;
                gw = Some(parse_ipv6_addr(args.get(i))?);
            }
            "--dns1" => {
                i += 1;
                dns1 = Some(parse_ipv6_addr(args.get(i))?);
            }
            "--dns2" => {
                i += 1;
                dns2 = Some(parse_ipv6_addr(args.get(i))?);
            }
            other => return Err(format!("未知选项 `{other}`（{USAGE}）")),
        }
        i += 1;
    }

    if dhcp && ip.is_some() {
        return Err("--dhcp 与 --ip 互斥".to_string());
    }
    if !dhcp && (ip.is_none() || prefix_len.is_none()) {
        return Err("静态模式需要 --ip 与 --prefix".to_string());
    }

    let info = Ipv6Info {
        enabled: true,
        use_dhcp: dhcp,
        auto_dns: dhcp,
        ip: ip.unwrap_or(Ipv6Addr::UNSPECIFIED),
        prefix_len: prefix_len.unwrap_or(0),
        gateway: gw.unwrap_or(Ipv6Addr::UNSPECIFIED),
        primary_dns: dns1.unwrap_or(Ipv6Addr::UNSPECIFIED),
        secondary_dns: dns2.unwrap_or(Ipv6Addr::UNSPECIFIED),
    };

    Ok(Command::Ipv6 { iface, info })
}

fn parse_ipv4_addr(s: Option<&&str>) -> Result<Ipv4Addr, String> {
    let s = s.ok_or("缺少 IP 地址参数")?;
    s.parse::<Ipv4Addr>()
        .map_err(|e| format!("无效的 IPv4 地址 `{s}`: {e}"))
}

fn parse_ipv6_addr(s: Option<&&str>) -> Result<Ipv6Addr, String> {
    let s = s.ok_or("缺少 IP 地址参数")?;
    s.parse::<Ipv6Addr>()
        .map_err(|e| format!("无效的 IPv6 地址 `{s}`: {e}"))
}

/// 执行命令并输出结果。
async fn execute(handle: &NetworkHandle, cmd: Command) -> anyhow::Result<()> {
    match cmd {
        Command::List => {
            let cards = handle.get_all_netcards().await?;
            print_list(&cards);
        }
        Command::Info { iface } => {
            let card = handle.get_netcard_info(&iface).await?;
            print_info(&card);
        }
        Command::Link { iface, up } => {
            handle.set_link_state(&iface, up).await?;
            println!("ok: {} -> {}", iface, if up { "up" } else { "down" });
        }
        Command::Ipv4 { iface, info } => {
            handle.set_ipv4_info(&iface, info).await?;
            println!("ok: 已应用 IPv4 配置到 {}", iface);
        }
        Command::Ipv6 { iface, info } => {
            handle.set_ipv6_info(&iface, info).await?;
            println!("ok: 已应用 IPv6 配置到 {}", iface);
        }
        Command::Help | Command::Quit | Command::Noop => unreachable!(),
    }
    Ok(())
}

fn print_list(cards: &[NetCardInfo]) {
    if cards.is_empty() {
        println!("(没有网卡)");
        return;
    }
    println!("{:<14} {:<14} IPV4", "NAME", "LINK");
    for c in cards {
        println!(
            "{:<14} {:<14} {}",
            c.name,
            format!("{:?}", c.hardware.state),
            c.ipv4.ip
        );
    }
}

fn print_info(card: &NetCardInfo) {
    println!("{card:#?}");
}

fn print_event(ev: &NetCardEvent) {
    let (tag, info) = match ev {
        NetCardEvent::Added(c) => ("ADD", c),
        NetCardEvent::Removed(c) => ("DEL", c),
        NetCardEvent::Changed(c) => ("CHG", c),
    };
    println!(
        "[{tag}] {:<14} link={:<14} ipv4={}",
        info.name,
        format!("{:?}", info.hardware.state),
        info.ipv4.ip
    );
}

fn print_banner() {
    println!("netman 交互式网络管理（输入 help 查看帮助，quit 退出）");
    println!("网卡事件会实时显示在本终端。");
}

fn print_help() {
    println!(concat!(
        "可用命令：\n",
        "  list | ls                              列出所有网卡\n",
        "  info <iface>                           查看网卡详情\n",
        "  link <iface> up|down                   启用/禁用网口链路\n",
        "  ipv4 <iface> --dhcp                    IPv4 自动获取（DHCP）\n",
        "  ipv4 <iface> --ip <ip> --netmask <m> [--gw <gw>] [--dns1 <d1>] [--dns2 <d2>]   静态 IPv4\n",
        "  ipv6 <iface> --dhcp                    IPv6 自动获取（DHCP）\n",
        "  ipv6 <iface> --ip <ip> --prefix <n> [--gw <gw>] [--dns1 <d1>] [--dns2 <d2>]   静态 IPv6\n",
        "  help | h                              显示本帮助\n",
        "  quit | exit | q                       退出"
    ));
}
