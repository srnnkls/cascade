//! An in-memory [`CredentialStore`] for the CLI tests.

use std::cell::RefCell;
use std::collections::HashMap;

use anyhow::Result;
use cascade_backend_smb::credentials::{CredentialStore, PasswordLocation};

type ItemKey = (String, String, Option<u16>);

#[derive(Default)]
pub struct MemoryStore(RefCell<HashMap<ItemKey, String>>);

impl MemoryStore {
    pub fn password(&self, host: &str, user: &str, port: Option<u16>) -> Option<String> {
        self.0
            .borrow()
            .get(&(host.to_owned(), user.to_owned(), port))
            .cloned()
    }
}

impl CredentialStore for MemoryStore {
    fn find(&self, host: &str, user: &str, port: Option<u16>) -> Result<Option<String>> {
        Ok(self.password(host, user, port))
    }

    fn store(
        &self,
        host: &str,
        user: &str,
        port: Option<u16>,
        password: &str,
    ) -> Result<PasswordLocation> {
        self.0.borrow_mut().insert(
            (host.to_owned(), user.to_owned(), port),
            password.to_owned(),
        );
        Ok(PasswordLocation::Keychain)
    }
}
