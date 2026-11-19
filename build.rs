use std::path::Path;

fn main() {
    println!("cargo:rerun-if-env-changed=GDRIVE_DEFAULT_CLIENT_ID");
    println!("cargo:rerun-if-changed=.env.build");

    // Runtime environment variables are intentionally not embedded into release builds.
    // A matching local .env.build pair is allowed for locally distributed packages.
    let env_id_set = std::env::var("GDRIVE_DEFAULT_CLIENT_ID")
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false);

    if !env_id_set {
        let env_file = Path::new(".env.build");
        if env_file.exists() {
            if let Ok(contents) = std::fs::read_to_string(env_file) {
                let mut client_id = None;
                let mut client_secret = None;
                for line in contents.lines() {
                    let trimmed = line.trim();
                    if trimmed.is_empty() || trimmed.starts_with('#') {
                        continue;
                    }
                    if let Some((key, val)) = trimmed.split_once('=') {
                        let key = key.trim();
                        let val = val.trim().trim_matches('"').trim_matches('\'');
                        if key == "GDRIVE_DEFAULT_CLIENT_ID" {
                            client_id = Some(val);
                        } else if key == "GDRIVE_DEFAULT_CLIENT_SECRET" {
                            client_secret = Some(val);
                        }
                    }
                }
                if let (Some(client_id), Some(client_secret)) = (client_id, client_secret) {
                    if !client_id.is_empty()
                        && !matches!(
                            client_id,
                            "prefilled-id.apps.googleusercontent.com"
                                | "my-id.apps.googleusercontent.com"
                                | "placeholder-id.apps.googleusercontent.com"
                                | "config-id-placeholder"
                                | "cli-id-placeholder"
                        )
                        && !client_secret.is_empty()
                    {
                        println!("cargo:rustc-env=GDRIVE_DEFAULT_CLIENT_ID={client_id}");
                        println!("cargo:rustc-env=GDRIVE_DEFAULT_CLIENT_SECRET={client_secret}");
                    }
                }
            }
        }
    }
}
