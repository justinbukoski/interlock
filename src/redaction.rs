use regex::{Captures, Regex};
use serde_json::Value;
use std::sync::LazyLock;

static BEARER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]{12,}").expect("static regex is valid")
});
static PROVIDER_KEY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\b(?:sk-(?:proj-)?[A-Za-z0-9_-]{16,}|sk-ant-[A-Za-z0-9_-]{16,}|AKIA[A-Z0-9]{16})\b",
    )
    .expect("static regex is valid")
});
static ASSIGNED_SECRET: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)\b(password|passwd|api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|session[_-]?cookie)["']?\s*[:=]\s*["']?([^\s,"';}]{6,})"#)
        .expect("static regex is valid")
});
static EMAIL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+\b")
        .expect("static regex is valid")
});
static CARD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(?:\d[ -]*?){13,19}\b").expect("static regex is valid"));
static JWT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b")
        .expect("static regex is valid")
});
static SSN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").expect("static regex is valid"));
static PHONE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:\+1[ .-]?)?(?:\(\d{3}\)|\d{3})[ .-]\d{3}[ .-]\d{4}\b")
        .expect("static regex is valid")
});
static CREDENTIAL_URI: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis|https?)://[^\s/@:]+:[^\s/@]+@[^\s]+")
        .expect("static regex is valid")
});
static PRIVATE_KEY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----.*?-----END (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")
        .expect("static regex is valid")
});

fn luhn_valid(value: &str) -> bool {
    let digits: Vec<u32> = value
        .chars()
        .filter_map(|character| character.to_digit(10))
        .collect();
    if !(13..=19).contains(&digits.len()) {
        return false;
    }
    digits
        .iter()
        .rev()
        .enumerate()
        .map(|(index, digit)| {
            if index % 2 == 1 {
                let doubled = digit * 2;
                if doubled > 9 { doubled - 9 } else { doubled }
            } else {
                *digit
            }
        })
        .sum::<u32>()
        % 10
        == 0
}

pub fn redact(input: &str) -> (String, usize) {
    let mut count = 0usize;
    let mut value = BEARER
        .replace_all(input, |captures: &Captures<'_>| {
            count += 1;
            format!("{}[REDACTED_TOKEN]", &captures[1])
        })
        .into_owned();
    value = PROVIDER_KEY
        .replace_all(&value, |_: &Captures<'_>| {
            count += 1;
            "[REDACTED_API_KEY]".to_owned()
        })
        .into_owned();
    value = ASSIGNED_SECRET
        .replace_all(&value, |captures: &Captures<'_>| {
            count += 1;
            format!("{}=[REDACTED_SECRET]", &captures[1])
        })
        .into_owned();
    for (pattern, label) in [
        (&*PRIVATE_KEY, "[REDACTED_PRIVATE_KEY]"),
        (&*CREDENTIAL_URI, "[REDACTED_CREDENTIAL_URI]"),
        (&*JWT, "[REDACTED_JWT]"),
        (&*SSN, "[REDACTED_SSN]"),
        (&*PHONE, "[REDACTED_PHONE]"),
    ] {
        value = pattern
            .replace_all(&value, |_: &Captures<'_>| {
                count += 1;
                label.to_owned()
            })
            .into_owned();
    }
    value = EMAIL
        .replace_all(&value, |_: &Captures<'_>| {
            count += 1;
            "[REDACTED_EMAIL]".to_owned()
        })
        .into_owned();
    value = CARD
        .replace_all(&value, |captures: &Captures<'_>| {
            if luhn_valid(&captures[0]) {
                count += 1;
                "[REDACTED_NUMBER]".to_owned()
            } else {
                captures[0].to_owned()
            }
        })
        .into_owned();
    (value, count)
}

pub fn contains_sensitive_text(value: &str) -> bool {
    redact(value).1 > 0
}

pub fn contains_sensitive_json(value: &Value) -> bool {
    match value {
        Value::Object(entries) => entries.iter().any(|(key, value)| {
            sensitive_key(key) || contains_sensitive_text(key) || contains_sensitive_json(value)
        }),
        Value::Array(values) => values.iter().any(contains_sensitive_json),
        Value::String(value) => contains_sensitive_text(value),
        Value::Number(value) => contains_sensitive_text(&value.to_string()),
        Value::Bool(_) | Value::Null => false,
    }
}

fn sensitive_key(value: &str) -> bool {
    let normalized: String = value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    matches!(
        normalized.as_str(),
        "password"
            | "passwd"
            | "apikey"
            | "accesstoken"
            | "refreshtoken"
            | "clientsecret"
            | "sessioncookie"
            | "privatekey"
            | "credential"
            | "credentials"
    )
}

#[cfg(test)]
mod tests {
    use super::{contains_sensitive_json, redact};
    use serde_json::json;

    #[test]
    fn removes_supported_secret_classes_without_echoing_values() {
        let input = "Bearer abcdefghijklmnopqrstuvwxyz password=hunter2x user@example.com sk-ant-abcdefghijklmnop 4111 1111 1111 1111";
        let (output, count) = redact(input);
        assert_eq!(count, 5);
        for secret in [
            "abcdefghijklmnopqrstuvwxyz",
            "hunter2x",
            "user@example.com",
            "sk-ant-abcdefghijklmnop",
            "4111 1111 1111 1111",
        ] {
            assert!(!output.contains(secret));
        }
    }

    #[test]
    fn leaves_operational_non_secrets_intact() {
        let input =
            "Interlock listens on 127.0.0.1:8851, commit 54f5ec0 passed, event 1234567890123";
        assert_eq!(redact(input), (input.to_owned(), 0));
    }

    #[test]
    fn removes_extended_sensitive_classes() {
        let input = "SSN 123-45-6789 phone (415) 555-1212 jwt eyJabcdefgh.ijklmnop.qrstuvwx postgres://alice:secret@db/internal";
        let (output, count) = redact(input);
        assert_eq!(count, 4);
        assert!(!output.contains("123-45-6789"));
        assert!(!output.contains("415"));
        assert!(!output.contains("eyJabcdefgh"));
        assert!(!output.contains("alice:secret"));
    }

    #[test]
    fn detects_nested_structured_secret_keys() {
        assert!(contains_sensitive_json(&json!({
            "config": {"password": "hunter2x"}
        })));
        assert!(contains_sensitive_json(&json!({
            "items": [{"refresh_token": "low-entropy"}]
        })));
        assert!(!contains_sensitive_json(&json!({
            "project": {"state": "tests passing"}
        })));
    }
}
