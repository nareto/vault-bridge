use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use hkdf::Hkdf;
use pbkdf2::pbkdf2_hmac;
use serde_json::Value;
use sha2::Sha256;
use thiserror::Error;

/// Prefix used by LiveSync HKDF-encrypted payloads.
const HKDF_ENCRYPTED_PREFIX: &str = "%=";

/// Prefix used by LiveSync for encrypted file metadata paths.
const ENCRYPTED_META_PREFIX: &str = "/\\:";

/// AES-GCM IV length in bytes (96 bits).
const IV_LENGTH: usize = 12;

/// HKDF session salt length in bytes.
const HKDF_SALT_LENGTH: usize = 32;

/// PBKDF2 iteration count (matches LiveSync's OWASP recommendation).
const PBKDF2_ITERATIONS: u32 = 310_000;

/// Decrypts and encrypts LiveSync HKDF-encrypted (v2) payloads.
///
/// Holds a PBKDF2-derived master key so the expensive derivation runs only
/// once at startup. Per-chunk HKDF derivation is cheap.
#[derive(Debug)]
pub struct Decryptor {
    /// Raw 32-byte master key derived via PBKDF2(passphrase, pbkdf2_salt).
    master_key: [u8; 32],
}

#[derive(Debug, Error)]
pub enum DecryptionError {
    #[error("missing '{0}' prefix on encrypted payload")]
    MissingPrefix(&'static str),
    #[error("base64 decode failed: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("encrypted payload too short (need at least {expected} bytes, got {got})")]
    TooShort { expected: usize, got: usize },
    #[error("AES-GCM decryption failed (wrong passphrase or corrupted data)")]
    DecryptFailed,
    #[error("decrypted bytes are not valid UTF-8: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
}

#[derive(Debug, Error)]
pub enum EncryptionError {
    #[error("failed to serialize encrypted metadata: {0}")]
    Json(#[from] serde_json::Error),
    #[error("AES-GCM encryption failed")]
    EncryptFailed,
}

impl Decryptor {
    /// Derive the master key from a passphrase and PBKDF2 salt.
    ///
    /// The salt is a 32-byte value fetched from CouchDB's
    /// `_local/obsidian_livesync_sync_parameters` document.
    pub fn new(passphrase: &str, pbkdf2_salt: &[u8]) -> Self {
        let mut master_key = [0u8; 32];
        pbkdf2_hmac::<Sha256>(
            passphrase.as_bytes(),
            pbkdf2_salt,
            PBKDF2_ITERATIONS,
            &mut master_key,
        );
        Self { master_key }
    }

    /// Decrypt a `%=`-prefixed, base64-encoded HKDF payload and return the
    /// plaintext as a UTF-8 string.
    ///
    /// Wire format (after base64 decode):
    /// `[12-byte IV][32-byte HKDF salt][ciphertext + 16-byte GCM tag]`
    pub fn decrypt(&self, data: &str) -> Result<String, DecryptionError> {
        let encoded = data
            .strip_prefix(HKDF_ENCRYPTED_PREFIX)
            .ok_or(DecryptionError::MissingPrefix(HKDF_ENCRYPTED_PREFIX))?;

        let raw = BASE64.decode(encoded)?;
        self.decrypt_binary(&raw)
    }

    /// Decrypt a `%=`-prefixed leaf data payload, or if it also carries the
    /// `/\:` metadata-encryption prefix, decrypt the embedded JSON and return
    /// only the `path` field.
    ///
    /// File metadata documents encrypt `{path, mtime, ctime, size, children}`
    /// as a JSON blob prefixed with `/\:`.  We decrypt that JSON and extract
    /// the real vault path.
    pub fn decrypt_meta_document(&self, raw_path: &str) -> Result<Value, DecryptionError> {
        let encrypted = raw_path
            .strip_prefix(ENCRYPTED_META_PREFIX)
            .ok_or(DecryptionError::MissingPrefix(ENCRYPTED_META_PREFIX))?;

        // The remaining part is a %=-prefixed HKDF payload.
        let json_str = self.decrypt(encrypted)?;

        serde_json::from_str(&json_str).map_err(|_| DecryptionError::DecryptFailed)
    }

    pub fn decrypt_meta_path(&self, raw_path: &str) -> Result<String, DecryptionError> {
        // Parse the JSON to extract the `path` field.
        let parsed = self.decrypt_meta_document(raw_path)?;
        parsed
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or(DecryptionError::DecryptFailed)
    }

    /// Encrypt plaintext using the same HKDF + AES-GCM envelope LiveSync uses.
    pub fn encrypt(&self, plaintext: &str) -> Result<String, EncryptionError> {
        let hkdf_salt: [u8; HKDF_SALT_LENGTH] = rand_bytes();
        let iv: [u8; IV_LENGTH] = rand_bytes();

        let hk = Hkdf::<Sha256>::new(Some(&hkdf_salt), &self.master_key);
        let mut aes_key = [0u8; 32];
        hk.expand(&[], &mut aes_key)
            .map_err(|_| EncryptionError::EncryptFailed)?;

        let cipher =
            Aes256Gcm::new_from_slice(&aes_key).map_err(|_| EncryptionError::EncryptFailed)?;
        let nonce = Nonce::from_slice(&iv);
        let ciphertext = cipher
            .encrypt(nonce, plaintext.as_bytes())
            .map_err(|_| EncryptionError::EncryptFailed)?;

        let mut buf = Vec::with_capacity(IV_LENGTH + HKDF_SALT_LENGTH + ciphertext.len());
        buf.extend_from_slice(&iv);
        buf.extend_from_slice(&hkdf_salt);
        buf.extend_from_slice(&ciphertext);

        Ok(format!("{HKDF_ENCRYPTED_PREFIX}{}", BASE64.encode(&buf)))
    }

    /// Encrypt the metadata blob LiveSync stores in file document `path`.
    pub fn encrypt_meta_path(
        &self,
        path: &str,
        mtime: i64,
        ctime: i64,
        size: i64,
        children: &[Value],
    ) -> Result<String, EncryptionError> {
        let metadata = serde_json::json!({
            "path": path,
            "mtime": mtime,
            "ctime": ctime,
            "size": size,
            "children": children,
        });
        let encrypted = self.encrypt(&serde_json::to_string(&metadata)?)?;
        Ok(format!("{ENCRYPTED_META_PREFIX}{encrypted}"))
    }

    /// Low-level: decrypt raw binary (already base64-decoded).
    fn decrypt_binary(&self, raw: &[u8]) -> Result<String, DecryptionError> {
        let min_len = IV_LENGTH + HKDF_SALT_LENGTH;
        if raw.len() < min_len {
            return Err(DecryptionError::TooShort {
                expected: min_len,
                got: raw.len(),
            });
        }

        let iv = &raw[..IV_LENGTH];
        let hkdf_salt = &raw[IV_LENGTH..IV_LENGTH + HKDF_SALT_LENGTH];
        let ciphertext = &raw[IV_LENGTH + HKDF_SALT_LENGTH..];

        // Derive per-chunk AES key via HKDF(master_key, hkdf_salt).
        let hk = Hkdf::<Sha256>::new(Some(hkdf_salt), &self.master_key);
        let mut aes_key = [0u8; 32];
        hk.expand(&[], &mut aes_key)
            .map_err(|_| DecryptionError::DecryptFailed)?;

        let cipher =
            Aes256Gcm::new_from_slice(&aes_key).map_err(|_| DecryptionError::DecryptFailed)?;
        let nonce = Nonce::from_slice(iv);
        let plaintext = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| DecryptionError::DecryptFailed)?;

        Ok(String::from_utf8(plaintext)?)
    }
}

fn rand_bytes<const N: usize>() -> [u8; N] {
    use aes_gcm::aead::OsRng;

    let mut buf = [0u8; N];
    aes_gcm::aead::rand_core::RngCore::fill_bytes(&mut OsRng, &mut buf);
    buf
}

/// Returns `true` if the string looks like a LiveSync HKDF-encrypted payload.
pub fn is_hkdf_encrypted(data: &str) -> bool {
    data.starts_with(HKDF_ENCRYPTED_PREFIX)
}

/// Returns `true` if the path looks like LiveSync encrypted file metadata.
pub fn is_encrypted_meta_path(path: &str) -> bool {
    path.starts_with(ENCRYPTED_META_PREFIX)
}

/// Convenience: decrypt if the value looks encrypted, otherwise return as-is.
pub fn maybe_decrypt(decryptor: Option<&Decryptor>, data: &str) -> Result<String, DecryptionError> {
    match decryptor {
        Some(d) if is_hkdf_encrypted(data) => d.decrypt(data),
        _ => Ok(data.to_string()),
    }
}

/// Convenience: decrypt file metadata path if encrypted, otherwise return as-is.
pub fn maybe_decrypt_meta_path(
    decryptor: Option<&Decryptor>,
    path: &str,
) -> Result<String, DecryptionError> {
    match decryptor {
        Some(d) if is_encrypted_meta_path(path) => d.decrypt_meta_path(path),
        _ => Ok(path.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_decryptor() -> (Decryptor, [u8; 32]) {
        let passphrase = "test-passphrase";
        let pbkdf2_salt = [0x42u8; 32];
        let d = Decryptor::new(passphrase, &pbkdf2_salt);
        (d, pbkdf2_salt)
    }

    #[test]
    fn round_trip_encrypt_decrypt() {
        let (d, _) = test_decryptor();
        let original = "# Hello World\n\nThis is a test note with unicode: äöü 🦀";
        let encrypted = d.encrypt(original).unwrap();

        assert!(encrypted.starts_with("%="));
        let decrypted = d.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn missing_prefix_error() {
        let (d, _) = test_decryptor();
        let err = d.decrypt("no-prefix-here").unwrap_err();
        assert!(matches!(err, DecryptionError::MissingPrefix(_)));
    }

    #[test]
    fn too_short_payload_error() {
        let (d, _) = test_decryptor();
        // Valid prefix but only a few bytes after base64 decode.
        let short = format!("%={}", BASE64.encode([0u8; 10]));
        let err = d.decrypt(&short).unwrap_err();
        assert!(matches!(err, DecryptionError::TooShort { .. }));
    }

    #[test]
    fn wrong_passphrase_fails() {
        let (d, salt) = test_decryptor();
        let encrypted = d.encrypt("secret content").unwrap();

        let wrong = Decryptor::new("wrong-passphrase", &salt);
        let err = wrong.decrypt(&encrypted).unwrap_err();
        assert!(matches!(err, DecryptionError::DecryptFailed));
    }

    #[test]
    fn is_hkdf_encrypted_detects_prefix() {
        assert!(is_hkdf_encrypted("%=abc123"));
        assert!(!is_hkdf_encrypted("plain text"));
        assert!(!is_hkdf_encrypted(""));
    }

    #[test]
    fn is_encrypted_meta_path_detects_prefix() {
        assert!(is_encrypted_meta_path("/\\:something"));
        assert!(!is_encrypted_meta_path("normal/path.md"));
    }

    #[test]
    fn maybe_decrypt_passthrough_when_no_prefix() {
        let (d, _) = test_decryptor();
        let result = maybe_decrypt(Some(&d), "plain text").unwrap();
        assert_eq!(result, "plain text");
    }

    #[test]
    fn maybe_decrypt_passthrough_when_no_decryptor() {
        let result = maybe_decrypt(None, "%=encrypted-looking").unwrap();
        assert_eq!(result, "%=encrypted-looking");
    }

    #[test]
    fn decrypt_meta_path_round_trip() {
        let (d, _) = test_decryptor();
        let meta_json = r#"{"path":"03Concepts/rust-phantom-types.md","mtime":1234567890,"ctime":1234567800,"size":42}"#;
        let encrypted_json = d.encrypt(meta_json).unwrap();
        let full_path = format!("/\\:{}", encrypted_json);

        let decrypted_path = d.decrypt_meta_path(&full_path).unwrap();
        assert_eq!(decrypted_path, "03Concepts/rust-phantom-types.md");
    }

    #[test]
    fn encrypt_meta_path_round_trip() {
        let (d, _) = test_decryptor();
        let children = vec![Value::String("h:+child".to_string())];
        let encrypted = d
            .encrypt_meta_path("00New/encrypted.md", 1234, 1200, 42, &children)
            .unwrap();

        assert!(encrypted.starts_with("/\\:%="));
        assert_eq!(
            d.decrypt_meta_path(&encrypted).unwrap(),
            "00New/encrypted.md"
        );

        let decrypted_json = d.decrypt(encrypted.strip_prefix("/\\:").unwrap()).unwrap();
        let parsed: Value = serde_json::from_str(&decrypted_json).unwrap();
        assert_eq!(parsed["mtime"], 1234);
        assert_eq!(parsed["ctime"], 1200);
        assert_eq!(parsed["size"], 42);
        assert_eq!(parsed["children"][0], "h:+child");
    }
}
