use eva_common::acl::Acl;
use eva_common::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ACI {
    auth: AuthMode,
    acl: String,
    token_mode: Option<TokenMode>,
    u: Option<String>,
    src: Option<String>,
}

impl ACI {
    #[inline]
    pub fn acl_id(&self) -> &str {
        &self.acl
    }
    #[inline]
    pub fn token_mode(&self) -> Option<TokenMode> {
        self.token_mode
    }
    #[inline]
    pub fn user(&self) -> Option<&str> {
        self.u.as_deref()
    }
    #[inline]
    pub fn source(&self) -> Option<&str> {
        self.src.as_deref()
    }
    #[inline]
    pub fn writable(&self) -> bool {
        if let Some(token_mode) = self.token_mode {
            token_mode == TokenMode::Normal
        } else {
            true
        }
    }
    #[inline]
    pub fn check_write(&self) -> EResult<()> {
        if self.writable() {
            Ok(())
        } else {
            Err(Error::access("Session is in the read-only mode"))
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TokenMode {
    Normal,
    Readonly,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    Token,
    Key,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct XParamsOwned {
    pub method: String,
    pub params: Value,
    pub aci: ACI,
    pub acl: Acl,
}

impl XParamsOwned {
    #[inline]
    pub fn method(&self) -> &str {
        &self.method
    }
}
