//! SMB backend configuration.

/// Configuration for an SMB backend.
#[derive(Clone)]
pub struct SmbConfig {
    /// Server host name or address.
    pub host: String,
    /// Share name on the server.
    pub share: String,
    /// TCP port; `None` uses the SMB default (445).
    pub port: Option<u16>,
    /// User name for NTLM authentication.
    pub username: String,
    /// Password for NTLM authentication.
    pub password: String,
    /// Optional sub-directory of the share to serve (no leading or trailing slash).
    pub prefix: Option<String>,
}

impl std::fmt::Debug for SmbConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmbConfig")
            .field("host", &self.host)
            .field("share", &self.share)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

pub fn parse_smb_config(config: &toml::Value) -> anyhow::Result<SmbConfig> {
    let get_str = |key: &str| -> anyhow::Result<String> {
        config
            .get(key)
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| anyhow::anyhow!("SMB backend requires '{key}' config"))
    };

    let port = config
        .get("port")
        .and_then(toml::Value::as_integer)
        .map(u16::try_from)
        .transpose()
        .map_err(|_| anyhow::anyhow!("SMB backend 'port' must be between 0 and 65535"))?;

    Ok(SmbConfig {
        host: get_str("host")?,
        share: get_str("share")?,
        port,
        username: get_str("username")?,
        password: get_str("password")?,
        prefix: config
            .get("prefix")
            .and_then(|v| v.as_str())
            .map(|s| s.trim_matches('/'))
            .filter(|s| !s.is_empty())
            .map(String::from),
    })
}
