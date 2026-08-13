use crate::{Ipv4Info, Ipv6Info};

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// 持久化根目录（所有网卡配置文件存放于此）。
const PERSIST_DIR: &str = "/root";

/// 一张网卡的完整持久化内容：统一持久化所有字段。
#[derive(Serialize, Deserialize, Default)]
pub struct NetCardStore {
    /// link 开关：true=up, false=down。
    #[serde(default)]
    link_up: bool,
    #[serde(default)]
    ipv4: Ipv4Info,
    #[serde(default)]
    ipv6: Ipv6Info,
}

impl NetCardStore {
    pub fn new(link_up: bool, ipv4: &Ipv4Info, ipv6: &Ipv6Info) -> Self {
        Self {
            link_up,
            ipv4: ipv4.clone(),
            ipv6: ipv6.clone(),
        }
    }

    pub fn link_up(&self) -> bool {
        self.link_up
    }

    pub fn ipv4(&self) -> Ipv4Info {
        self.ipv4.clone()
    }

    pub fn ipv6(&self) -> Ipv6Info {
        self.ipv6.clone()
    }
}

// ---------------- 文件路径与原子写入 ----------------

/// 返回指定网卡的配置文件路径：`/root/<iface_name>.json`
fn config_path(iface_name: &str) -> PathBuf {
    PathBuf::from(PERSIST_DIR).join(format!("{}.json", iface_name))
}

/// 原子写入：先写到临时文件并 sync，再 rename 覆盖目标文件。
fn atomic_write(path: &Path, content: &str) -> std::io::Result<()> {
    use std::fs::OpenOptions;
    use std::io::Write;

    // 确保目录存在
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let tmp_path = path.with_extension("json.tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

/// 加载某张网卡的持久化配置。文件不存在或解析失败时返回 `None`。
pub fn load(iface_name: &str) -> Option<NetCardStore> {
    let path = config_path(iface_name);
    let data = std::fs::read(&path).ok()?;
    serde_json::from_slice::<NetCardStore>(&data).ok()
}

/// 持久化完整配置（link 开关 + IPv4 + IPv6）。
pub fn save_full(
    iface_name: &str,
    link_up: bool,
    ipv4: &Ipv4Info,
    ipv6: &Ipv6Info,
) -> std::io::Result<()> {
    let config = NetCardStore::new(link_up, ipv4, ipv6);
    let json = serde_json::to_string_pretty(&config).map_err(std::io::Error::other)?;
    atomic_write(&config_path(iface_name), &json)
}
