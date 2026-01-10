use eva_common::op::Op;
use eva_common::prelude::*;
use hyper::{Body, StatusCode, Uri, client::HttpConnector};
use hyper_tls::HttpsConnector;
use serde::{Deserialize, Serialize};
use simple_pool::ResourcePool;
use std::collections::BTreeMap;
use std::time::Duration;

type Resource = hyper::Client<HttpsConnector<HttpConnector>>;

pub const MAX_REDIRECTS: usize = 10;

pub struct Client {
    pool: ResourcePool<Resource>,
    timeout: Duration,
    max_redirects: usize,
    follow_redirects: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

impl Response {
    #[inline]
    pub fn status(&self) -> u16 {
        self.status
    }
    #[inline]
    pub fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }
    #[inline]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

impl TryFrom<Response> for hyper::http::Response<Body> {
    type Error = Error;
    fn try_from(resp: Response) -> EResult<Self> {
        let mut r = hyper::http::Response::builder();
        for (header, value) in resp.headers {
            r = r.header(header, value);
        }
        r.status(StatusCode::from_u16(resp.status).map_err(Error::failed)?)
            .body(Body::from(resp.body))
            .map_err(Error::failed)
    }
}

impl Client {
    pub fn new(pool_size: usize, timeout: Duration) -> Self {
        let pool: ResourcePool<Resource> = <_>::default();
        for _ in 0..=pool_size {
            let https = HttpsConnector::new();
            let client: hyper::Client<_> = hyper::Client::builder()
                .pool_idle_timeout(timeout)
                .build(https);
            pool.append(client);
        }
        Self {
            pool,
            timeout,
            max_redirects: MAX_REDIRECTS,
            follow_redirects: true,
        }
    }
    #[inline]
    pub fn max_redirects(mut self, max_redirects: usize) -> Self {
        self.max_redirects = max_redirects;
        self
    }
    #[inline]
    pub fn follow_redirects(mut self, follow: bool) -> Self {
        self.follow_redirects = follow;
        self
    }
    pub async fn get(&self, url: &str) -> EResult<hyper::Response<Body>> {
        let op = Op::new(self.timeout);
        let mut target_uri: Uri = {
            if url.starts_with("http://") || url.starts_with("https://") {
                url.parse()
            } else {
                format!("http://{url}").parse()
            }
        }
        .map_err(|e| Error::invalid_params(format!("invalid url {}: {}", url, e)))?;
        let client = tokio::time::timeout(op.timeout()?, self.pool.get()).await?;
        let mut rdr = 0;
        loop {
            let res = tokio::time::timeout(op.timeout()?, client.get(target_uri.clone()))
                .await?
                .map_err(Error::io)?;
            if self.follow_redirects
                && (res.status() == StatusCode::MOVED_PERMANENTLY
                    || res.status() == StatusCode::TEMPORARY_REDIRECT
                    || res.status() == StatusCode::FOUND)
            {
                if rdr > self.max_redirects {
                    return Err(Error::io("too many redirects"));
                }
                rdr += 1;
                if let Some(loc) = res.headers().get(hyper::header::LOCATION) {
                    let location_uri: Uri = loc
                        .to_str()
                        .map_err(|e| Error::invalid_params(format!("invalid redirect url: {e}")))?
                        .parse()
                        .map_err(|e| Error::invalid_params(format!("invalid redirect url: {e}")))?;
                    let loc_parts = location_uri.into_parts();
                    let mut parts = target_uri.into_parts();
                    if loc_parts.scheme.is_some() {
                        parts.scheme = loc_parts.scheme;
                    }
                    if loc_parts.authority.is_some() {
                        parts.authority = loc_parts.authority;
                    }
                    parts.path_and_query = loc_parts.path_and_query;
                    target_uri = Uri::from_parts(parts)
                        .map_err(|e| Error::invalid_params(format!("invalid redirect url: {e}")))?;
                } else {
                    return Err(Error::io("invalid redirect"));
                }
            } else {
                return Ok(res);
            }
        }
    }
    pub async fn get_response(&self, url: &str) -> EResult<Response> {
        let op = Op::new(self.timeout);
        let resp = self.get(url).await?;
        let status = resp.status().as_u16();
        let mut headers = BTreeMap::new();
        for (header, value) in resp.headers() {
            headers.insert(
                header.to_string(),
                value.to_str().unwrap_or_default().to_owned(),
            );
        }
        let body = tokio::time::timeout(op.timeout()?, hyper::body::to_bytes(resp))
            .await?
            .map_err(Error::io)?
            .to_vec();
        Ok(Response {
            status,
            headers,
            body,
        })
    }
}
