// Allow disallowed methods for this module because RustEmbed generates calls to Path::canonicalize
#![allow(clippy::disallowed_methods)]

use crate::profile_setup::ProfileSetup;
use dbt_common::pretty_string::{GREEN, YELLOW};
use dbt_common::{ErrorCode, FsResult, fs_err};
use rust_embed::RustEmbed;
use std::env;
use std::fs;
use std::path::Path;

#[derive(RustEmbed)]
#[folder = "assets/jaffle_shop/"]
struct ProjectTemplate;

/// Create or update .vscode/extensions.json file with dbt extension recommendation
fn create_or_update_vscode_extensions(target_dir: &Path) -> FsResult<()> {
    let vscode_dir = target_dir.join(".vscode");
    let extensions_file = vscode_dir.join("extensions.json");

    // Create .vscode directory if it doesn't exist
    fs::create_dir_all(&vscode_dir)?;

    let dbt_extension = "dbtLabsInc.dbt";

    if extensions_file.exists() {
        // File exists, read and check if our extension is already there
        let content = fs::read_to_string(&extensions_file)?;

        // Parse the JSON to check if our extension is already present
        let mut json: serde_json::Value = serde_json::from_str(&content)
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to parse extensions.json: {}", e))?;

        // Ensure we have a recommendations array
        if !json.is_object() {
            json = serde_json::json!({});
        }

        let mut recommendations = json
            .get("recommendations")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_else(Vec::new);

        // Check if our extension is already in the list
        let already_exists = recommendations
            .iter()
            .any(|item| item.as_str() == Some(dbt_extension));

        if !already_exists {
            recommendations.push(serde_json::Value::String(dbt_extension.to_string()));
            json["recommendations"] = serde_json::Value::Array(recommendations);

            // Write back the updated content with pretty formatting
            let updated_content = serde_json::to_string_pretty(&json).map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to serialize extensions.json: {}",
                    e
                )
            })?;
            fs::write(&extensions_file, updated_content)?;

            log::info!(
                "{} Added dbt extension recommendation to existing .vscode/extensions.json",
                GREEN.apply_to("Info")
            );
        } else {
            log::info!(
                "{} dbt extension already recommended in .vscode/extensions.json, skipping",
                YELLOW.apply_to("Info")
            );
        }
    } else {
        // File doesn't exist, create it with our extension
        let extensions_json = serde_json::json!({
            "recommendations": [
                dbt_extension
            ]
        });
        let extensions_content = serde_json::to_string_pretty(&extensions_json).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to serialize extensions.json: {}",
                e
            )
        })?;
        fs::write(&extensions_file, extensions_content)?;

        log::info!(
            "{} Created .vscode/extensions.json with dbt extension recommendation",
            GREEN.apply_to("Info")
        );
    }

    Ok(())
}

pub fn init_project(project_name: &str, target_dir: &Path) -> FsResult<()> {
    fs::create_dir_all(target_dir)?;

    // Extract all embedded files
    for file_path in ProjectTemplate::iter() {
        let file_content = ProjectTemplate::get(&file_path).ok_or_else(|| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to read embedded file: {}",
                file_path
            )
        })?;

        let target_file_path = target_dir.join(file_path.as_ref());

        // Create parent directories if they don't exist
        if let Some(parent) = target_file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Read the content as string and replace project name placeholder
        let content = String::from_utf8_lossy(&file_content.data);
        let content = content.replace("jaffle_shop", project_name);

        // Write the file
        fs::write(&target_file_path, content)?;
    }

    Ok(())
}

pub fn get_profiles_dir() -> String {
    // Try environment variable first, then fall back to default
    env::var("DBT_PROFILES_DIR").unwrap_or_else(|_| {
        let home = env::var("HOME").unwrap_or_else(|_| ".".to_string());
        format!("{home}/.dbt")
    })
}

/// Check if we're currently in a dbt project directory
pub fn is_in_dbt_project() -> bool {
    Path::new("dbt_project.yml").exists()
}

/// Get the profile name from dbt_project.yml if we're in a project
pub fn get_profile_name_from_project() -> FsResult<String> {
    let content = fs::read_to_string("dbt_project.yml")?;
    let project: serde_json::Value = dbt_serde_yaml::from_str(&content)
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to parse dbt_project.yml: {}", e))?;

    if let Some(profile_value) = project.get("profile") {
        if let Some(profile_str) = profile_value.as_str() {
            return Ok(profile_str.to_string());
        }
    }

    // No profile found in dbt_project.yml, ask user to provide one
    use dialoguer::Input;

    let default_profile = project
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("my_profile");

    let profile_name: String = Input::new()
        .with_prompt(
            "No profile found in dbt_project.yml. What profile name would you like to use?",
        )
        .default(default_profile.to_string())
        .interact_text()
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get profile name: {}", e))?;

    // Now update the dbt_project.yml file to include the profile
    update_dbt_project_profile(&profile_name)?;

    Ok(profile_name)
}

/// Update dbt_project.yml to include the profile field
fn update_dbt_project_profile(profile_name: &str) -> FsResult<()> {
    let content = fs::read_to_string("dbt_project.yml")?;

    // Simple approach: find the first line that starts with a field name and add profile before it
    // or add it after the 'name:' field if found
    let lines: Vec<&str> = content.lines().collect();
    let mut new_lines = Vec::new();
    let mut profile_added = false;

    for line in lines.iter() {
        new_lines.push(line.to_string());

        // If we find the 'name:' field, add the profile field right after it
        if !profile_added && line.trim_start().starts_with("name:") {
            new_lines.push(format!("profile: {profile_name}"));
            profile_added = true;
        }
        // If we find another top-level field (not name) and haven't added profile yet, add it before
        else if !profile_added
            && !line.trim().is_empty()
            && !line.starts_with(' ')
            && !line.starts_with('\t')
            && !line.starts_with('#')
            && !line.trim_start().starts_with("name:")
            && line.contains(':')
        {
            // Insert profile before this line
            new_lines.pop(); // Remove the current line
            new_lines.push(format!("profile: {profile_name}"));
            new_lines.push(line.to_string());
            profile_added = true;
        }
    }

    // If we still haven't added it, add it at the end
    if !profile_added {
        new_lines.push(format!("profile: {profile_name}"));
    }

    let updated_content = new_lines.join("\n");
    if !updated_content.ends_with('\n') {
        fs::write("dbt_project.yml", updated_content + "\n")?;
    } else {
        fs::write("dbt_project.yml", updated_content)?;
    }

    log::info!(
        "{} Added profile '{}' to dbt_project.yml",
        GREEN.apply_to("Success"),
        profile_name
    );

    Ok(())
}

/// Check if a profile exists in profiles.yml
pub fn check_if_profile_exists(profile_name: &str, profiles_dir: &str) -> FsResult<bool> {
    let profiles_file = Path::new(profiles_dir).join("profiles.yml");
    if !profiles_file.exists() {
        return Ok(false);
    }

    let content = fs::read_to_string(profiles_file)?;
    let profiles: serde_json::Value = dbt_serde_yaml::from_str(&content)
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to parse profiles.yml: {}", e))?;

    Ok(profiles.get(profile_name).is_some())
}

/// Main init workflow that handles both project creation and profile setup
/// プロジェクトの作成とプロファイルの設定の両方を処理するメインの初期化ワークフロー
pub async fn run_init_workflow(
    project_name: Option<String>,
    skip_profile_setup: bool,
    existing_profile: Option<String>,
) -> FsResult<()> {
    let profiles_dir = get_profiles_dir();
    let profile_setup = ProfileSetup::new(profiles_dir.clone());

    let inside_existing_project = is_in_dbt_project();

    // Determine whether the user explicitly provided a project name.
    // ユーザーがプロジェクト名を明示的に指定したかどうかを判断します。
    let (mut project_name, user_specified_project_name) = match project_name {
        Some(name) => (name, true),
        None => ("jaffle_shop".to_string(), false),
    };

    // CASE 1: Inside an existing project **and** the user did NOT provide a new project name →
    // behave like dbt-core: only set up (or update) a profile.
    // ケース 1: 既存のプロジェクト内で、**かつ** ユーザーが新しいプロジェクト名を指定しなかった場合 → 
    // dbt-core のように動作し、プロファイルの設定 (または更新) のみを行います。
    if inside_existing_project && !user_specified_project_name {
        if existing_profile.is_some() {
            return Err(fs_err!(
                ErrorCode::InvalidArgument,
                "Cannot init existing project with specified profile, edit dbt_project.yml instead"
            ));
        }

        log::info!(
            "{} A dbt_project.yml already exists in this directory; skipping sample project creation.",
            YELLOW.apply_to("Warning")
        );

        // Create or update .vscode/extensions.json even when skipping project creation
        // プロジェクトの作成をスキップした場合でも .vscode/extensions.json を作成または更新する
        create_or_update_vscode_extensions(Path::new("."))?;

        if !skip_profile_setup {
            let profile_name = get_profile_name_from_project()?;
            profile_setup.setup_profile(&profile_name).await?;
        }

        return Ok(());
    }

    // CASE 2: Either we're not in a project, **or** the user asked for a new project explicitly –
    // proceed to create the sample project directory.
    // ケース 2: プロジェクト内にいないか、**または** ユーザーが明示的に新しいプロジェクトを要求しました。
    // サンプル プロジェクト ディレクトリの作成に進みます。

    {
        // If the chosen project directory already exists, find the next available
        // 選択したプロジェクトディレクトリがすでに存在する場合は、次に利用可能なディレクトリを検索します。
        if Path::new(&project_name).exists() {
            let unique_name = next_available_dir_name(&project_name);
            log::info!(
                "{} Directory '{}' already exists, using '{}' instead",
                YELLOW.apply_to("Warning"),
                project_name,
                YELLOW.apply_to(&unique_name)
            );
            project_name = unique_name;
        }

        // Validate profile if specified
        // 指定されている場合はプロファイルを検証します
        if let Some(ref profile_name) = existing_profile {
            if !check_if_profile_exists(profile_name, &profiles_dir)? {
                return Err(fs_err!(
                    ErrorCode::InvalidArgument,
                    "Could not find profile named '{}'",
                    profile_name
                ));
            }
        }

        // Create the project
        // プロジェクトを作成する
        let project_dir = Path::new(&project_name);
        init_project(&project_name, project_dir)?;

        // Create or update .vscode/extensions.json in the new project
        // 新しいプロジェクトで.vscode/extensions.jsonを作成または更新します。
        create_or_update_vscode_extensions(project_dir)?;

        // Change to project directory
        // プロジェクトディレクトリに変更
        env::set_current_dir(&project_name)?;

        log::info!(
            "{} Project created successfully!",
            GREEN.apply_to("Success")
        );
        log::info!("{} Project name: {}", GREEN.apply_to("Info"), project_name);
        log::info!(
            "{} Project directory: {}",
            GREEN.apply_to("Info"),
            project_dir.display()
        );

        // Setup profile if not skipped
        // スキップしない場合はプロファイルを設定する
        if !skip_profile_setup {
            let profile_name = existing_profile
                .as_ref()
                .cloned()
                .unwrap_or_else(|| project_name.clone());

            // Only run profile setup if we don't have an existing profile specified
            // 既存のプロファイルが指定されていない場合にのみプロファイル設定を実行します
            if existing_profile.is_none() {
                profile_setup.setup_profile(&profile_name).await?;
            }
        }
    }

    Ok(())
}

/// Given a base directory name, return the first `{base}_{n}` (n starting at 1) that does not
/// already exist. If none of the suffixed names exist it returns the base name itself.
fn next_available_dir_name(base: &str) -> String {
    let mut counter = 1;
    loop {
        let candidate = format!("{base}_{counter}");
        if !Path::new(&candidate).exists() {
            return candidate;
        }
        counter += 1;
    }
}
