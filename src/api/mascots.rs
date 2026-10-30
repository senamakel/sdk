//! Mascots: library, manifests, Rive assets, and the interactive demo.

use reqwest::Method;

use super::types::DynamicResponse;
use crate::{enc, Error, HttpClient};

/// Typed client for the `/mascots/*` routes.
pub struct MascotsApi<'a> {
    http: &'a HttpClient,
}

impl<'a> MascotsApi<'a> {
    pub fn new(http: &'a HttpClient) -> Self {
        Self { http }
    }

    /// List available mascots.
    pub async fn list_mascots(&self) -> Result<DynamicResponse, Error> {
        self.http
            .send_typed(Method::GET, "/mascots", &[], None, true)
            .await
    }

    /// Interactive mascot library demo page.
    pub async fn get_demo(&self) -> Result<DynamicResponse, Error> {
        self.http
            .send_typed(Method::GET, "/mascots/demo", &[], None, true)
            .await
    }

    /// Get a mascot manifest.
    pub async fn get_mascot(&self, id: &str) -> Result<DynamicResponse, Error> {
        let path = format!("/mascots/{}", enc(id));
        self.http
            .send_typed(Method::GET, &path, &[], None, true)
            .await
    }

    /// Download the Rive animation file for a mascot.
    pub async fn get_mascot_riv(&self, id: &str) -> Result<DynamicResponse, Error> {
        let path = format!("/mascots/{}/riv", enc(id));
        self.http
            .send_typed(Method::GET, &path, &[], None, true)
            .await
    }
}
