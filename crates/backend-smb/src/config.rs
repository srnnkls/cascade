//! SMB backend configuration.

use crate::credentials::CredentialStore;

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

/// Parses an SMB backend's TOML. Without a `password` key the password is
/// looked up in `store`.
///
/// # Errors
///
/// Returns an error if a required key is missing, `password` is empty, the
/// port is out of range, or the store has no password or fails.
pub fn parse_smb_config(
    config: &toml::Value,
    store: &dyn CredentialStore,
) -> anyhow::Result<SmbConfig> {
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

    let host = get_str("host")?;
    let share = get_str("share")?;
    let username = get_str("username")?;
    let password = match config.get("password") {
        Some(value) => {
            let password = value
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("SMB backend 'password' must be a string"))?;
            if password.is_empty() {
                anyhow::bail!("SMB backend 'password' must not be empty");
            }
            password.to_owned()
        }
        None => stored_password(store, &host, &username, port)?,
    };

    Ok(SmbConfig {
        host,
        share,
        port,
        username,
        password,
        prefix: config
            .get("prefix")
            .and_then(|v| v.as_str())
            .map(|s| s.trim_matches('/'))
            .filter(|s| !s.is_empty())
            .map(String::from),
    })
}

fn stored_password(
    store: &dyn CredentialStore,
    host: &str,
    user: &str,
    port: Option<u16>,
) -> anyhow::Result<String> {
    store.find(host, user, port)?.ok_or_else(|| {
        anyhow::anyhow!(
            "no SMB password of {user} on {host} in the keychain; store it with \
                 `cascade backend-add` or in Keychain Access"
        )
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;

    use super::parse_smb_config;
    use crate::credentials::{CredentialStore, PasswordLocation};

    type ItemKey = (String, String, Option<u16>);

    #[derive(Default)]
    struct MemoryStore(RefCell<HashMap<ItemKey, String>>);

    impl MemoryStore {
        fn holding(host: &str, user: &str, port: Option<u16>, password: &str) -> Self {
            let store = Self::default();
            store.store(host, user, port, password).unwrap();
            store
        }
    }

    impl CredentialStore for MemoryStore {
        fn find(
            &self,
            host: &str,
            user: &str,
            port: Option<u16>,
        ) -> anyhow::Result<Option<String>> {
            Ok(self
                .0
                .borrow()
                .get(&(host.to_owned(), user.to_owned(), port))
                .cloned())
        }

        fn store(
            &self,
            host: &str,
            user: &str,
            port: Option<u16>,
            password: &str,
        ) -> anyhow::Result<PasswordLocation> {
            self.0.borrow_mut().insert(
                (host.to_owned(), user.to_owned(), port),
                password.to_owned(),
            );
            Ok(PasswordLocation::Keychain)
        }
    }

    fn config(extra: &str) -> toml::Value {
        toml::from_str(&format!(
            "host = \"nas.local\"\nshare = \"media\"\nusername = \"alice\"\n{extra}"
        ))
        .unwrap()
    }

    #[test]
    fn password_in_the_toml_wins_over_the_store_and_must_not_be_empty() {
        let store = MemoryStore::holding("nas.local", "alice", None, "from-store");

        let parsed = parse_smb_config(&config("password = \"from-toml\""), &store).unwrap();

        assert_eq!(parsed.password, "from-toml");
        assert!(parse_smb_config(&config("password = \"\""), &store).is_err());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn a_store_error_other_than_not_found_keeps_its_message() {
        struct LockedStore;
        impl CredentialStore for LockedStore {
            fn find(&self, _: &str, _: &str, _: Option<u16>) -> anyhow::Result<Option<String>> {
                anyhow::bail!("The user name or passphrase you entered is not correct.")
            }
            fn store(
                &self,
                _: &str,
                _: &str,
                _: Option<u16>,
                _: &str,
            ) -> anyhow::Result<PasswordLocation> {
                anyhow::bail!("not used")
            }
        }

        let msg = format!(
            "{:#}",
            parse_smb_config(&config(""), &LockedStore).unwrap_err()
        );

        assert!(
            msg.contains("passphrase you entered is not correct"),
            "error was: {msg}"
        );
        assert!(!msg.contains("cascade backend-add"), "error was: {msg}");
    }

    #[cfg(target_os = "macos")]
    mod keychain {
        use security_framework::os::macos::keychain::{CreateOptions, SecKeychain};
        use security_framework::os::macos::passwords::{
            SecAuthenticationType, SecProtocolType, find_internet_password,
        };

        use super::{config, parse_smb_config};
        use crate::credentials::{CredentialStore, KeychainStore};

        /// A keychain file of its own in a temporary directory, never on the
        /// user's search list; removed with the directory.
        fn temp_keychain() -> (tempfile::TempDir, SecKeychain) {
            let dir = tempfile::tempdir().unwrap();
            let keychain = CreateOptions::new()
                .password("cascade-test")
                .create(dir.path().join("smb-test.keychain"))
                .unwrap();
            (dir, keychain)
        }

        fn add_item(
            keychain: &SecKeychain,
            host: &str,
            user: &str,
            port: Option<u16>,
            protocol: SecProtocolType,
            password: &str,
        ) {
            keychain
                .add_internet_password(
                    host,
                    None,
                    user,
                    "",
                    port,
                    protocol,
                    SecAuthenticationType::Default,
                    password.as_bytes(),
                )
                .unwrap();
        }

        fn smb_item(keychain: &SecKeychain, port: Option<u16>) -> Option<String> {
            find_internet_password(
                Some(std::slice::from_ref(keychain)),
                "nas.local",
                None,
                "alice",
                "",
                port,
                SecProtocolType::SMB,
                SecAuthenticationType::Any,
            )
            .ok()
            .map(|(password, _)| String::from_utf8(password.to_owned()).unwrap())
        }

        #[test]
        fn lookup_matches_server_account_smb_protocol_and_configured_port() {
            let _no_dialogs = SecKeychain::disable_user_interaction().unwrap();
            let (_dir, keychain) = temp_keychain();
            add_item(
                &keychain,
                "nas.local",
                "alice",
                None,
                SecProtocolType::HTTPS,
                "https",
            );
            add_item(
                &keychain,
                "files.local",
                "alice",
                None,
                SecProtocolType::SMB,
                "host",
            );
            add_item(
                &keychain,
                "nas.local",
                "bob",
                None,
                SecProtocolType::SMB,
                "user",
            );
            let store = KeychainStore::new(vec![keychain.clone()]);

            let msg = format!("{:#}", parse_smb_config(&config(""), &store).unwrap_err());
            assert!(
                msg.contains("nas.local") && msg.contains("alice"),
                "error was: {msg}"
            );
            assert!(msg.contains("cascade backend-add"), "error was: {msg}");
            assert!(msg.contains("Keychain Access"), "error was: {msg}");

            add_item(
                &keychain,
                "nas.local",
                "alice",
                None,
                SecProtocolType::SMB,
                "finder",
            );
            assert_eq!(
                parse_smb_config(&config(""), &store).unwrap().password,
                "finder"
            );
            assert!(parse_smb_config(&config("port = 4445"), &store).is_err());

            add_item(
                &keychain,
                "nas.local",
                "alice",
                Some(4445),
                SecProtocolType::SMB,
                "4445",
            );
            assert_eq!(
                parse_smb_config(&config("port = 4445"), &store)
                    .unwrap()
                    .password,
                "4445"
            );
        }

        fn find(store: &KeychainStore, port: Option<u16>) -> Option<String> {
            store.find("nas.local", "alice", port).unwrap()
        }

        #[test]
        fn store_adds_or_updates_the_smb_item_on_port_445_for_the_default() {
            let _no_dialogs = SecKeychain::disable_user_interaction().unwrap();
            let (_dir, keychain) = temp_keychain();
            let store = KeychainStore::new(vec![keychain.clone()]);

            store.store("nas.local", "alice", None, "first").unwrap();
            store.store("nas.local", "alice", None, "second").unwrap();

            assert_eq!(smb_item(&keychain, None).as_deref(), Some("second"));
            assert_eq!(smb_item(&keychain, Some(445)).as_deref(), Some("second"));
            assert_eq!(
                parse_smb_config(&config(""), &store).unwrap().password,
                "second"
            );

            store
                .store("nas.local", "alice", Some(4445), "on-4445")
                .unwrap();
            assert_eq!(smb_item(&keychain, Some(4445)).as_deref(), Some("on-4445"));
        }

        #[test]
        fn storing_for_the_default_port_leaves_an_item_of_another_port_alone() {
            let _no_dialogs = SecKeychain::disable_user_interaction().unwrap();
            let (_dir, keychain) = temp_keychain();
            let store = KeychainStore::new(vec![keychain]);

            store
                .store("nas.local", "alice", Some(4445), "on-4445")
                .unwrap();
            store
                .store("nas.local", "alice", None, "on-default")
                .unwrap();

            assert_eq!(find(&store, Some(4445)).as_deref(), Some("on-4445"));
            assert_eq!(find(&store, None).as_deref(), Some("on-default"));
        }

        #[test]
        fn storing_for_another_port_leaves_the_default_port_item_alone() {
            let _no_dialogs = SecKeychain::disable_user_interaction().unwrap();
            let (_dir, keychain) = temp_keychain();
            let store = KeychainStore::new(vec![keychain]);

            store
                .store("nas.local", "alice", None, "on-default")
                .unwrap();
            store
                .store("nas.local", "alice", Some(4445), "on-4445")
                .unwrap();

            assert_eq!(find(&store, Some(4445)).as_deref(), Some("on-4445"));
            assert_eq!(find(&store, None).as_deref(), Some("on-default"));
        }

        #[test]
        fn a_finder_item_without_a_port_is_found_and_kept_when_storing_for_the_default() {
            let _no_dialogs = SecKeychain::disable_user_interaction().unwrap();
            let (_dir, keychain) = temp_keychain();
            add_item(
                &keychain,
                "nas.local",
                "alice",
                None,
                SecProtocolType::SMB,
                "finder",
            );
            let store = KeychainStore::new(vec![keychain.clone()]);

            assert_eq!(find(&store, None).as_deref(), Some("finder"));

            store.store("nas.local", "alice", None, "cascade").unwrap();
            assert_eq!(find(&store, None).as_deref(), Some("cascade"));

            let (_, on_445) = find_internet_password(
                Some(std::slice::from_ref(&keychain)),
                "nas.local",
                None,
                "alice",
                "",
                Some(445),
                SecProtocolType::SMB,
                SecAuthenticationType::Any,
            )
            .unwrap();
            on_445.delete();
            assert_eq!(smb_item(&keychain, None).as_deref(), Some("finder"));
        }

        #[test]
        fn another_port_does_not_fall_back_to_an_item_without_a_port() {
            let _no_dialogs = SecKeychain::disable_user_interaction().unwrap();
            let (_dir, keychain) = temp_keychain();
            add_item(
                &keychain,
                "nas.local",
                "alice",
                None,
                SecProtocolType::SMB,
                "finder",
            );
            let store = KeychainStore::new(vec![keychain]);

            assert_eq!(find(&store, Some(4445)), None);
        }

        #[test]
        fn port_445_is_the_default_port() {
            let _no_dialogs = SecKeychain::disable_user_interaction().unwrap();
            let (_dir, keychain) = temp_keychain();
            let store = KeychainStore::new(vec![keychain]);

            store
                .store("nas.local", "alice", Some(445), "on-445")
                .unwrap();

            assert_eq!(find(&store, None).as_deref(), Some("on-445"));
        }
    }
}
