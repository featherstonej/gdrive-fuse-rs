use std::path::Path;

fn main() {
    println!("cargo:rerun-if-env-changed=GDRIVE_DEFAULT_CLIENT_ID");
    println!("cargo:rerun-if-changed=.env.build");

    // If GDRIVE_DEFAULT_CLIENT_ID is not already set in the environment, load from .env.build if present.
    let env_id_set = std::env::var("GDRIVE_DEFAULT_CLIENT_ID")
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false);

    if !env_id_set {
        let env_file = Path::new(".env.build");
        if env_file.exists() {
            if let Ok(contents) = std::fs::read_to_string(env_file) {
                for line in contents.lines() {
                    let trimmed = line.trim();
                    if trimmed.is_empty() || trimmed.starts_with('#') {
                        continue;
                    }
                    if let Some((key, val)) = trimmed.split_once('=') {
                        let key = key.trim();
                        let val = val.trim().trim_matches('"').trim_matches('\'');
                        if key == "GDRIVE_DEFAULT_CLIENT_ID" && !val.is_empty() {
                            println!("cargo:rustc-env={}={}", key, val);
                        }
                    }
                }
            }
        }
    }
}
