//! Where an SMB password lives when the backend's TOML has no `password` key.
//!
//! On macOS that is an internet password in the keychain: server = host,
//! account = user name, protocol SMB, port = the configured port or 445, the
//! kind of item Finder saves for a share. Lookup for the default port falls
//! back to an item without a port, as Finder saves it. Other platforms have no
//! credential store.

/// Where [`CredentialStore::store`] left the password.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PasswordLocation {
    /// The store holds it; the backend's TOML has no `password` key.
    Keychain,
    /// The store cannot hold it; the caller keeps it as `password` in the
    /// backend's TOML.
    Toml,
}

/// Lookup and storage of SMB passwords by host, user and port.
pub trait CredentialStore {
    /// The password for `user` on `host`, `Ok(None)` when no item matches.
    ///
    /// # Errors
    ///
    /// Returns an error for any store failure other than a missing item.
    fn find(&self, host: &str, user: &str, port: Option<u16>) -> anyhow::Result<Option<String>>;

    /// Adds the item or updates its password, and says where the password
    /// went.
    ///
    /// # Errors
    ///
    /// Returns an error when the store refuses the item.
    fn store(
        &self,
        host: &str,
        user: &str,
        port: Option<u16>,
        password: &str,
    ) -> anyhow::Result<PasswordLocation>;
}

/// The store the CLI and the daemon use: the user's default keychain search
/// list on macOS, [`NoStore`] elsewhere.
#[must_use]
pub fn system_store() -> impl CredentialStore {
    #[cfg(target_os = "macos")]
    {
        KeychainStore::default_list()
    }
    #[cfg(not(target_os = "macos"))]
    {
        NoStore
    }
}

/// A platform without a credential store: the password belongs in the
/// backend's TOML.
#[derive(Debug, Clone, Copy)]
pub struct NoStore;

impl CredentialStore for NoStore {
    fn find(&self, _: &str, _: &str, _: Option<u16>) -> anyhow::Result<Option<String>> {
        anyhow::bail!("SMB backend requires 'password' in the backend's TOML file")
    }

    fn store(&self, _: &str, _: &str, _: Option<u16>, _: &str) -> anyhow::Result<PasswordLocation> {
        Ok(PasswordLocation::Toml)
    }
}

#[cfg(target_os = "macos")]
pub use keychain::KeychainStore;

#[cfg(target_os = "macos")]
mod keychain {
    use anyhow::Context as _;
    use security_framework::os::macos::keychain::SecKeychain;
    use security_framework::os::macos::passwords::{
        SecAuthenticationType, SecProtocolType, find_internet_password,
    };

    use super::{CredentialStore, PasswordLocation};

    /// `errSecItemNotFound` of the Security framework.
    const ERR_SEC_ITEM_NOT_FOUND: i32 = -25300;

    const DEFAULT_SMB_PORT: u16 = 445;

    /// SMB internet passwords in the macOS keychain.
    pub struct KeychainStore {
        /// `None` searches the user's default search list and stores into the
        /// default keychain.
        search_list: Option<Vec<SecKeychain>>,
    }

    impl std::fmt::Debug for KeychainStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("KeychainStore")
                .field("keychains", &self.search_list.as_ref().map(Vec::len))
                .finish()
        }
    }

    impl KeychainStore {
        /// `find` searches these keychains; `store` writes into the first.
        #[must_use]
        pub const fn new(search_list: Vec<SecKeychain>) -> Self {
            Self {
                search_list: Some(search_list),
            }
        }

        /// The user's default search list; `store` writes into the default
        /// (login) keychain.
        #[must_use]
        pub const fn default_list() -> Self {
            Self { search_list: None }
        }
    }

    impl KeychainStore {
        /// `None` matches an item of any port, a port matches exactly.
        fn find_on(
            &self,
            host: &str,
            user: &str,
            port: Option<u16>,
        ) -> anyhow::Result<Option<String>> {
            match find_internet_password(
                self.search_list.as_deref(),
                host,
                None,
                user,
                "",
                port,
                SecProtocolType::SMB,
                SecAuthenticationType::Any,
            ) {
                Ok((password, _)) => {
                    String::from_utf8(password.to_vec())
                        .map(Some)
                        .with_context(|| {
                            format!("the keychain password of {user} on {host} is not UTF-8")
                        })
                }
                Err(err) if err.code() == ERR_SEC_ITEM_NOT_FOUND => Ok(None),
                Err(err) => Err(anyhow::Error::new(err).context(format!(
                    "reading the SMB password of {user} on {host} from the keychain"
                ))),
            }
        }
    }

    impl CredentialStore for KeychainStore {
        /// For the default port, an item on port 445 and else one without a
        /// port, as Finder saves it; that fallback also matches an item of
        /// another port when no other item exists for the host and user.
        fn find(
            &self,
            host: &str,
            user: &str,
            port: Option<u16>,
        ) -> anyhow::Result<Option<String>> {
            let port = port.unwrap_or(DEFAULT_SMB_PORT);
            match self.find_on(host, user, Some(port))? {
                None if port == DEFAULT_SMB_PORT => self.find_on(host, user, None),
                found => Ok(found),
            }
        }

        fn store(
            &self,
            host: &str,
            user: &str,
            port: Option<u16>,
            password: &str,
        ) -> anyhow::Result<PasswordLocation> {
            let keychain = match &self.search_list {
                Some(list) => list
                    .first()
                    .cloned()
                    .context("the keychain search list is empty")?,
                None => SecKeychain::default()?,
            };
            keychain.set_internet_password(
                host,
                None,
                user,
                "",
                Some(port.unwrap_or(DEFAULT_SMB_PORT)),
                SecProtocolType::SMB,
                SecAuthenticationType::Default,
                password.as_bytes(),
            )?;
            Ok(PasswordLocation::Keychain)
        }
    }
}
