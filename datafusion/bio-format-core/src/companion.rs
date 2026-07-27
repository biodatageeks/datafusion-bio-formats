//! Format-neutral companion-object naming and discovery.

use std::future::Future;
use std::path::{Path, PathBuf};

use datafusion::common::{DataFusionError, Result};
use url::Url;

/// A conventional transformation from a primary location to a companion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompanionRule {
    /// Append a suffix, such as `.csi`, to the complete primary path.
    AppendSuffix(String),
    /// Replace the last filename extension.
    ReplaceExtension(String),
    /// Replace an exact filename suffix with another suffix.
    ReplaceSuffix {
        /// Required primary filename suffix.
        from: String,
        /// Replacement companion filename suffix.
        to: String,
    },
}

/// Applies ordered companion rules and removes duplicate candidate locations.
pub fn companion_candidates(primary: &str, rules: &[CompanionRule]) -> Result<Vec<String>> {
    let mut candidates = Vec::with_capacity(rules.len());
    for rule in rules {
        let candidate = apply_rule(primary, rule)?;
        if !candidates.contains(&candidate) {
            candidates.push(candidate);
        }
    }
    Ok(candidates)
}

/// Resolves an explicit or conventional companion through a caller-supplied
/// asynchronous existence check.
///
/// The existence callback receives an owned location so it can safely cross an
/// async boundary. Explicit locations take precedence and fail immediately
/// when absent. Conventional candidates are tried in order.
pub async fn resolve_companion<F, Fut>(
    primary: &str,
    role: &str,
    explicit: Option<&str>,
    rules: &[CompanionRule],
    required: bool,
    mut exists: F,
) -> Result<Option<String>>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    if let Some(explicit) = explicit {
        if exists(explicit.to_string()).await? {
            return Ok(Some(explicit.to_string()));
        }
        return Err(DataFusionError::Plan(format!(
            "explicit {role} companion does not exist: {}",
            sanitize_location(explicit)
        )));
    }

    let candidates = companion_candidates(primary, rules)?;
    for candidate in &candidates {
        if exists(candidate.clone()).await? {
            return Ok(Some(candidate.clone()));
        }
    }

    if required {
        let attempted = candidates
            .iter()
            .map(|candidate| sanitize_location(candidate))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(DataFusionError::Plan(format!(
            "required {role} companion was not found; attempted: {attempted}"
        )));
    }

    Ok(None)
}

/// Removes URL credentials, query parameters, and fragments from an error-safe
/// location string.
pub fn sanitize_location(location: &str) -> String {
    let Ok(mut url) = Url::parse(location) else {
        return location.to_string();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

fn apply_rule(primary: &str, rule: &CompanionRule) -> Result<String> {
    if primary.contains("://") {
        let mut url = Url::parse(primary).map_err(|error| {
            DataFusionError::Plan(format!("invalid primary object URL: {error}"))
        })?;
        let path = transform_path(Path::new(url.path()), rule)?;
        let path = path.to_str().ok_or_else(|| {
            DataFusionError::Plan("companion URL path is not valid UTF-8".to_string())
        })?;
        url.set_path(path);
        url.set_query(None);
        url.set_fragment(None);
        Ok(url.to_string())
    } else {
        let transformed = transform_path(Path::new(primary), rule)?;
        transformed
            .to_str()
            .map(str::to_string)
            .ok_or_else(|| DataFusionError::Plan("companion path is not valid UTF-8".to_string()))
    }
}

fn transform_path(primary: &Path, rule: &CompanionRule) -> Result<PathBuf> {
    match rule {
        CompanionRule::AppendSuffix(suffix) => {
            let mut value = primary.as_os_str().to_os_string();
            value.push(suffix);
            Ok(PathBuf::from(value))
        }
        CompanionRule::ReplaceExtension(extension) => {
            let mut value = primary.to_path_buf();
            value.set_extension(extension.trim_start_matches('.'));
            Ok(value)
        }
        CompanionRule::ReplaceSuffix { from, to } => {
            let value = primary.to_str().ok_or_else(|| {
                DataFusionError::Plan("primary path is not valid UTF-8".to_string())
            })?;
            let stripped = value.strip_suffix(from).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "primary path {value} does not end with required suffix {from}"
                ))
            })?;
            Ok(PathBuf::from(format!("{stripped}{to}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_local_candidates_in_order() {
        let candidates = companion_candidates(
            "/data/cohort.bed",
            &[
                CompanionRule::ReplaceExtension("bim".to_string()),
                CompanionRule::AppendSuffix(".bim".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(candidates, vec!["/data/cohort.bim", "/data/cohort.bed.bim"]);
    }

    #[test]
    fn builds_remote_candidates_without_copying_query_credentials() {
        let candidates = companion_candidates(
            "https://example.test/data/cohort.bgen?signature=secret",
            &[CompanionRule::AppendSuffix(".bgi".to_string())],
        )
        .unwrap();
        assert_eq!(
            candidates,
            vec!["https://example.test/data/cohort.bgen.bgi"]
        );
    }

    #[test]
    fn sanitizes_url_for_errors() {
        assert_eq!(
            sanitize_location("https://user:password@example.test/a?token=secret#fragment"),
            "https://example.test/a"
        );
    }

    #[tokio::test]
    async fn explicit_location_takes_precedence() {
        let resolved = resolve_companion(
            "/data/cohort.bed",
            "BIM",
            Some("/other/variants.bim"),
            &[CompanionRule::ReplaceExtension("bim".to_string())],
            true,
            |candidate| async move { Ok(candidate == "/other/variants.bim") },
        )
        .await
        .unwrap();
        assert_eq!(resolved.as_deref(), Some("/other/variants.bim"));
    }

    #[tokio::test]
    async fn conventional_candidates_are_probed_in_order() {
        let resolved = resolve_companion(
            "/data/cohort.bed",
            "BIM",
            None,
            &[
                CompanionRule::AppendSuffix(".bim".to_string()),
                CompanionRule::ReplaceExtension("bim".to_string()),
            ],
            true,
            |candidate| async move { Ok(candidate == "/data/cohort.bim") },
        )
        .await
        .unwrap();
        assert_eq!(resolved.as_deref(), Some("/data/cohort.bim"));
    }

    #[tokio::test]
    async fn required_missing_companion_has_sanitized_error() {
        let error = resolve_companion(
            "https://example.test/cohort.bgen?token=secret",
            "BGI",
            None,
            &[CompanionRule::AppendSuffix(".bgi".to_string())],
            true,
            |_| async { Ok(false) },
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("https://example.test/cohort.bgen.bgi"));
        assert!(!error.contains("secret"));
    }
}
