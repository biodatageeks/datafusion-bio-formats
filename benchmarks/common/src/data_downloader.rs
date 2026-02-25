use anyhow::anyhow;
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const GDRIVE_BASE_URL: &str = "https://drive.google.com/uc?export=download&id=";
const GDRIVE_CONFIRM_URL: &str = "https://drive.google.com/uc?export=download&confirm=t&id=";

#[derive(Debug, Clone)]
pub struct TestDataFile {
    pub filename: String,
    pub drive_id: String,
    pub checksum: Option<String>,
}

impl TestDataFile {
    pub fn new(filename: impl Into<String>, drive_id: impl Into<String>) -> Self {
        Self {
            filename: filename.into(),
            drive_id: drive_id.into(),
            checksum: None,
        }
    }

    pub fn with_checksum(mut self, checksum: impl Into<String>) -> Self {
        self.checksum = Some(checksum.into());
        self
    }
}

pub struct DataDownloader {
    cache_dir: PathBuf,
}

impl DataDownloader {
    pub fn new() -> Result<Self> {
        let cache_dir = dirs::cache_dir()
            .ok_or_else(|| anyhow!("Could not determine cache directory"))?
            .join("datafusion-bio-benchmarks");

        std::fs::create_dir_all(&cache_dir)?;

        Ok(Self { cache_dir })
    }

    pub fn download(&self, file: &TestDataFile, force: bool) -> Result<PathBuf> {
        let output_path = self.cache_dir.join(&file.filename);

        if output_path.exists() && !force {
            // Validate cached file is not a stale HTML page from a previous failed download
            if let Err(e) = validate_not_html(&output_path, &file.filename) {
                println!("✗ Cached file invalid ({e}), re-downloading...");
                // validate_not_html already removes the file on HTML detection
            } else {
                println!("✓ Using cached file: {}", output_path.display());

                if let Some(expected_checksum) = &file.checksum {
                    let actual_checksum = calculate_sha256(&output_path)?;
                    if &actual_checksum != expected_checksum {
                        println!("✗ Checksum mismatch, re-downloading...");
                        std::fs::remove_file(&output_path)?;
                    } else {
                        return Ok(output_path);
                    }
                } else {
                    return Ok(output_path);
                }
            }
        }

        println!("Downloading {} from Google Drive...", file.filename);

        // Try direct download first
        if let Err(e) = self.download_direct(file, &output_path) {
            println!("Direct download failed ({e}), trying with confirmation...");
            self.download_with_confirmation(file, &output_path)?;
        }

        // Verify the downloaded file is not an HTML page (Google Drive sometimes
        // returns an HTML warning/quota page with HTTP 200 instead of the actual file)
        validate_not_html(&output_path, &file.filename)?;

        // Verify checksum if provided
        if let Some(expected_checksum) = &file.checksum {
            println!("Verifying checksum...");
            let actual_checksum = calculate_sha256(&output_path)?;
            if &actual_checksum != expected_checksum {
                std::fs::remove_file(&output_path)?;
                return Err(anyhow!(
                    "Checksum mismatch:\n  Expected: {expected_checksum}\n  Actual:   {actual_checksum}"
                ));
            }
            println!("✓ Checksum verified");
        }

        Ok(output_path)
    }

    fn download_direct(&self, file: &TestDataFile, output_path: &Path) -> Result<()> {
        let url = format!("{}{}", GDRIVE_BASE_URL, file.drive_id);
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()?;

        let response = client.get(&url).send()?;

        if !response.status().is_success() {
            return Err(anyhow!("HTTP error: {}", response.status()));
        }

        let total_size = response.content_length().unwrap_or(0);

        let pb = ProgressBar::new(total_size);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

        let mut file = File::create(output_path)?;
        let mut downloaded: u64 = 0;
        let mut reader = response;

        let mut buffer = vec![0; 8192];
        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            file.write_all(&buffer[..bytes_read])?;
            downloaded += bytes_read as u64;
            pb.set_position(downloaded);
        }

        pb.finish_with_message("Download complete");
        Ok(())
    }

    fn download_with_confirmation(&self, file: &TestDataFile, output_path: &Path) -> Result<()> {
        let url = format!("{}{}", GDRIVE_CONFIRM_URL, file.drive_id);
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()?;

        let response = client.get(&url).send()?;

        if !response.status().is_success() {
            return Err(anyhow!("HTTP error: {}", response.status()));
        }

        let total_size = response.content_length().unwrap_or(0);

        let pb = ProgressBar::new(total_size);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

        let mut file = File::create(output_path)?;
        let mut downloaded: u64 = 0;
        let mut reader = response;

        let mut buffer = vec![0; 8192];
        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            file.write_all(&buffer[..bytes_read])?;
            downloaded += bytes_read as u64;
            pb.set_position(downloaded);
        }

        pb.finish_with_message("Download complete");
        Ok(())
    }
}

pub fn extract_drive_id(url: &str) -> Result<String> {
    // Handle various Google Drive URL formats:
    // https://drive.google.com/file/d/{ID}/view?usp=drive_link
    // https://drive.google.com/file/d/{ID}/view
    // https://drive.google.com/uc?id={ID}

    if let Some(start) = url.find("/d/") {
        let id_start = start + 3;
        let remaining = &url[id_start..];

        if let Some(end) = remaining.find('/') {
            return Ok(remaining[..end].to_string());
        } else if let Some(end) = remaining.find('?') {
            return Ok(remaining[..end].to_string());
        } else {
            return Ok(remaining.to_string());
        }
    }

    if let Some(start) = url.find("id=") {
        let id_start = start + 3;
        let remaining = &url[id_start..];

        if let Some(end) = remaining.find('&') {
            return Ok(remaining[..end].to_string());
        } else {
            return Ok(remaining.to_string());
        }
    }

    Err(anyhow!("Could not extract Google Drive ID from URL: {url}"))
}

/// Check that a downloaded file is not an HTML page.
/// Google Drive sometimes returns HTTP 200 with an HTML warning or quota-exceeded
/// page instead of the actual file content.
fn validate_not_html(path: &Path, filename: &str) -> Result<()> {
    let mut file = File::open(path)?;
    let mut header = [0u8; 512];
    let bytes_read = file.read(&mut header)?;
    if bytes_read == 0 {
        anyhow::bail!(
            "Downloaded file '{filename}' is empty — Google Drive may have returned an error page"
        );
    }

    // Check for HTML markers in the first bytes
    let header_str = String::from_utf8_lossy(&header[..bytes_read]);
    let lower = header_str.to_lowercase();
    if lower.contains("<!doctype html") || lower.contains("<html") {
        // Remove the bogus file so it doesn't get cached
        let _ = std::fs::remove_file(path);
        anyhow::bail!(
            "Downloaded file '{}' is an HTML page instead of the expected data file. \
             This usually means the Google Drive download quota has been exceeded \
             or the file requires manual authorization. \
             Please download the file manually and place it at: {}",
            filename,
            path.display()
        );
    }

    Ok(())
}

pub fn calculate_sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path).context(format!("Failed to open file: {}", path.display()))?;

    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 8192];

    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}
