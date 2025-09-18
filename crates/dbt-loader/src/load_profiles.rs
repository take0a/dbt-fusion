use dbt_jinja_utils::jinja_environment::JinjaEnv;

use dbt_common::constants::{DBT_PROFILES_YML, LOADING};
use dbt_common::stdfs::canonicalize;
use dbt_common::{ErrorCode, FsResult, err, fs_err, fsinfo, show_progress, show_warning};

use pathdiff::diff_paths;
use std::path::PathBuf;

use dbt_schemas::schemas::project::DbtProjectSimplified;
use dbt_schemas::state::DbtProfile;

use dirs::home_dir;

use crate::args::{IoArgs, LoadArgs};
use crate::utils::read_profiles_and_extract_db_config;
use serde::Serialize;

pub fn load_profiles<S: Serialize>(
    arg: &LoadArgs,
    raw_dbt_project: &DbtProjectSimplified,
    jinja_env: &JinjaEnv,
    ctx: &S,
) -> FsResult<DbtProfile> {
    let profile_str = get_profile_string(
        &arg.io,
        arg.profile.as_ref(),
        raw_dbt_project.profile.as_ref(),
    )?;

    // TODO: Add Secret Renderer logic to profile renderer
    // TODO: プロファイル レンダラーに Secret Renderer ロジックを追加する

    // Load Profiles From ~/.dbt/profiles.yml and the dbt_project_dir
    // ~/.dbt/profiles.yml と dbt_project_dir からプロファイルをロードする
    let has_dbt_cloud_config_defined = if let Some(dbt_cloud) = raw_dbt_project.dbt_cloud.as_ref() {
        dbt_cloud.project_id.is_some()
    } else {
        false
    };
    let profile_path = get_profile_path(&arg.io, &arg.profiles_dir, has_dbt_cloud_config_defined)?;

    let abs_profile_path = canonicalize(&profile_path)?;
    let abs_in_dir = canonicalize(&arg.io.in_dir)?;
    let relative_profile_path = diff_paths(&abs_profile_path, &abs_in_dir).ok_or_else(|| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to get relative path from profiles.yml to project directory"
        )
    })?;

    let show_path = if let Some(home_dir) = home_dir() {
        let home_dir = home_dir.join(".dbt");
        if abs_profile_path.starts_with(home_dir) {
            PathBuf::from("~/.dbt/profiles.yml")
        } else {
            relative_profile_path.clone()
        }
    } else {
        relative_profile_path.clone()
    };

    show_progress!(
        arg.io,
        fsinfo!(LOADING.into(), show_path.display().to_string())
    );

    // Load just the keys -> values from the profiles.yml file
    // profiles.yml ファイルからキー -> 値のみをロードします
    let (target, db_config) = read_profiles_and_extract_db_config(
        &arg.io,
        &arg.target,
        jinja_env,
        ctx,
        &profile_str,
        profile_path,
    )?;

    // TODO: Certain databases enforce that database and schema are specified
    // TODO: 特定のデータベースでは、データベースとスキーマの指定が必須です
    let database = db_config
        .get_database()
        .map(String::as_str)
        .unwrap_or("dbt")
        .to_string();
    let schema = db_config
        .get_schema()
        .map(String::as_str)
        .unwrap_or("public")
        .to_string();

    Ok(DbtProfile {
        database,
        schema,
        profile: profile_str,
        target,
        db_config,
        relative_profile_path,
        threads: arg.threads,
    })
}

/// Resolve the profile name to use.
/// # Parameters
/// - `arg_profile`: the profile name provided via the `--profile` command-line argument. If present, this value takes precedence over the project file.
/// - `proj_profile`: the profile name provided via the `dbt_project.yml` file. Used as a fallback if `--profile` is not provided.
///
/// # Returns
/// - The profile name to use.
///
/// # Warnings
/// - If the profile is not specified in `dbt_project.yml` but `--profile` is provided.
fn get_profile_string(
    io_args: &IoArgs,
    arg_profile: Option<&String>,
    proj_profile: Option<&String>,
) -> FsResult<String> {
    match (proj_profile, arg_profile) {
        (None, None) => {
            err!(
                ErrorCode::InvalidConfig,
                "No profile specified in dbt_project.yml"
            )
        }
        (None, Some(prof)) => {
            show_warning!(
                &io_args,
                fs_err!(
                    ErrorCode::InvalidConfig,
                    "No profile specified in dbt_project.yml"
                )
            );
            Ok(prof.to_string())
        }
        (Some(proj_prof), None) => Ok(proj_prof.to_string()),
        (Some(_), Some(prof)) => Ok(prof.to_string()),
    }
}

/// Resolve the path to the profiles.yml file to use.
///
/// Search the following paths in order
/// - The path provided via the `--profiles_dir` (`dbt_profile_dir_override`)
/// - At the project root
/// - At the $HOME/.dbt/
fn get_profile_path(
    io_args: &IoArgs,
    dbt_profile_dir_override: &Option<PathBuf>,
    has_dbt_cloud_config_defined: bool,
) -> FsResult<PathBuf> {
    let dbt_cloud_not_supported_yet_message = if has_dbt_cloud_config_defined {
        "Cloud CLI credentials from `dbt_cloud.yml` are not yet supported."
    } else {
        ""
    };

    match dbt_profile_dir_override {
        Some(dbt_profile_dir_override) => {
            let maybe_profile_path = dbt_profile_dir_override.join(DBT_PROFILES_YML);
            if maybe_profile_path.exists() {
                Ok(maybe_profile_path)
            } else {
                err!(
                    ErrorCode::InvalidConfig,
                    "No profiles.yml found at `{}`. \n\n{} Try running without the --profiles-dir flag to check the default locations.",
                    maybe_profile_path.display(),
                    dbt_common::pretty_string::BLUE.apply_to("suggestion: ")
                )
            }
        }
        None => {
            let maybe_profile_path = io_args.in_dir.join(DBT_PROFILES_YML);
            if maybe_profile_path.exists() {
                Ok(maybe_profile_path)
            } else if let Some(home_path) = home_dir() {
                let dbt_home_profile_path = home_path.join(".dbt").join(DBT_PROFILES_YML);
                if dbt_home_profile_path.exists() {
                    Ok(dbt_home_profile_path)
                } else {
                    err!(
                        ErrorCode::InvalidConfig,
                        "No profiles.yml found at `{}` or `{}`. \n\n{}Run `dbt init` to create a profiles.yml file and connect to your database. {}",
                        dbt_home_profile_path.display(),
                        maybe_profile_path.display(),
                        dbt_common::pretty_string::BLUE.apply_to("suggestion: "),
                        dbt_cloud_not_supported_yet_message
                    )
                }
            } else {
                err!(
                    ErrorCode::InvalidConfig,
                    "No profiles.yml found in ~/.dbt, in project directory, or specified via --profiles-dir. \n\n{} Run `dbt init` to create a profiles.yml file and connect to your database. {}",
                    dbt_common::pretty_string::BLUE.apply_to("suggestion: "),
                    dbt_cloud_not_supported_yet_message
                )
            }
        }
    }
}
