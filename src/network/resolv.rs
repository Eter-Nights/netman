use anyhow::Result;
use std::fs;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::path::Path;

const RESOLV_PATH: &str = "/etc/resolv.conf";

/// 更新 IPv4 DNS 配置：覆盖该网卡已有的 IPv4 nameserver 条目。
pub fn update_ipv4_dns(iface_name: &str, primary: Ipv4Addr, secondary: Ipv4Addr) -> Result<()> {
    let p = fmt_ipv4_or_empty(primary);
    let s = fmt_ipv4_or_empty(secondary);
    update_internal(iface_name, p, s, true)
}

/// 更新 IPv6 DNS 配置：覆盖该网卡已有的 IPv6 nameserver 条目。
pub fn update_ipv6_dns(iface_name: &str, primary: Ipv6Addr, secondary: Ipv6Addr) -> Result<()> {
    let p = fmt_ipv6_or_empty(primary);
    let s = fmt_ipv6_or_empty(secondary);
    update_internal(iface_name, p, s, false)
}

/// 内部通用 DNS 更新逻辑：过滤掉当前网卡在同协议族上的旧条目，再追加新条目。
fn update_internal(
    iface_name: &str,
    primary_dns: String,
    secondary_dns: String,
    is_ipv4: bool,
) -> Result<()> {
    let path = Path::new(RESOLV_PATH);

    // 读取现有内容
    let existing_content = if path.exists() {
        fs::read_to_string(path)?
    } else {
        String::new()
    };

    // 过滤掉当前网卡且同协议族的 DNS 配置
    let marker = format!("#{}", iface_name);
    let mut new_lines = Vec::new();

    for line in existing_content.lines() {
        let trimmed = line.trim();

        if trimmed.starts_with("nameserver ") && trimmed.ends_with(&marker) {
            let ip_part = trimmed["nameserver ".len()..trimmed.len() - marker.len()].trim();
            let is_existing_ipv4 = ip_part.contains('.') && !ip_part.contains(':');

            if is_existing_ipv4 == is_ipv4 {
                continue;
            }
        }

        new_lines.push(line.to_string());
    }

    // 添加新的 DNS 配置（非空时）
    if !primary_dns.is_empty() {
        new_lines.push(format!("nameserver {} {}", primary_dns, marker));
    }
    if !secondary_dns.is_empty() {
        new_lines.push(format!("nameserver {} {}", secondary_dns, marker));
    }

    // 写回文件
    write_back(path, &new_lines)
}

/// 把 IPv4 地址转字符串；UNSPECIFIED 转成空串（表示该项不写入）。
fn fmt_ipv4_or_empty(addr: Ipv4Addr) -> String {
    if addr.is_unspecified() {
        String::new()
    } else {
        addr.to_string()
    }
}

/// 把 IPv6 地址转字符串；UNSPECIFIED 转成空串（表示该项不写入）。
fn fmt_ipv6_or_empty(addr: Ipv6Addr) -> String {
    if addr.is_unspecified() {
        String::new()
    } else {
        addr.to_string()
    }
}

/// 按行写入：空则写空串，非空则每行末尾补换行。
fn write_back(path: &Path, lines: &[String]) -> Result<()> {
    if lines.is_empty() {
        fs::write(path, "")?;
    } else {
        let mut content = lines.join("\n");
        content.push('\n');
        fs::write(path, content)?;
    }
    Ok(())
}
