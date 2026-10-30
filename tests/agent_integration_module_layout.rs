//! The agent-integration surface is split one module per provider. These tests
//! pin both halves of that layout: every provider module is reachable by name,
//! and the pre-split `api::agent_integration_types::*` path still resolves to
//! the same types, so vendored consumers (openhuman pins this crate as a
//! submodule) keep compiling across the move.

use tinyhumans_sdk::api::agent_integration_types as flat;
use tinyhumans_sdk::api::agent_integrations as split;

/// Each provider owns a module holding its DTOs and its `impl` block.
#[test]
fn every_provider_has_its_own_module() {
    fn assert_named<T>(_: std::marker::PhantomData<T>) {}

    assert_named::<split::composio::ComposioAuthorizeRequest>(std::marker::PhantomData);
    assert_named::<split::crypto::CryptoSwapRequest>(std::marker::PhantomData);
    assert_named::<split::file_storage::FileMetadata>(std::marker::PhantomData);
    assert_named::<split::financial_apis::QuoteResponse>(std::marker::PhantomData);
    assert_named::<split::gemini::GeminiGenerateContentRequest>(std::marker::PhantomData);
    assert_named::<split::google_places::GooglePlacesSearchRequest>(std::marker::PhantomData);
    assert_named::<split::history_rewards::HistoryRewardsStatus>(std::marker::PhantomData);
    assert_named::<split::media_generation::ImageGenerationRequest>(std::marker::PhantomData);
    assert_named::<split::openrouter::OpenRouterModel>(std::marker::PhantomData);
    assert_named::<split::openrouter_media::OpenRouterImageRequest>(std::marker::PhantomData);
    assert_named::<split::parallel::ParallelChatRequest>(std::marker::PhantomData);
    assert_named::<split::pricing::IntegrationPricingResponse>(std::marker::PhantomData);
    assert_named::<split::sarvam::SarvamTextToSpeechRequest>(std::marker::PhantomData);
    assert_named::<split::tenor::TenorSearchRequest>(std::marker::PhantomData);
    assert_named::<split::tinyfish::TinyFishSearchRequest>(std::marker::PhantomData);
}

/// The historical flat path must alias the very same types, not copies of them.
#[test]
fn the_pre_split_types_path_still_resolves() {
    fn assert_same_type<T>(_: &T, _: &T) {}

    assert_same_type(
        &flat::TenorSearchRequest {
            query: "cat".into(),
            limit: None,
            content_filter: None,
            pos: None,
        },
        &split::tenor::TenorSearchRequest {
            query: "cat".into(),
            limit: None,
            content_filter: None,
            pos: None,
        },
    );
    // The module-root glob re-export, which is what `AgentIntegrationsApi`
    // method signatures name.
    assert_same_type(
        &flat::IntegrationPricingResponse::default(),
        &split::IntegrationPricingResponse::default(),
    );
    assert_same_type(
        &flat::OpenRouterImageRequest::default(),
        &split::openrouter_media::OpenRouterImageRequest::default(),
    );
}
