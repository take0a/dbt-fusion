use dbt_common::adapter::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_jinja_utils::jinja_environment::JinjaEnv;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use dbt_common::constants::DBT_PROJECT_YML;

use dbt_common::stdfs;

use dbt_common::{ErrorCode, FsResult};
use dbt_common::{err, fs_err, show_warning};
use dbt_schemas::state::{DbtPackage, DbtProfile, DbtVars};

use crate::args::LoadArgs;
use crate::loader::load_inner;

mod assets {
    #![allow(clippy::disallowed_methods)] // RustEmbed generates calls to std::path::Path::canonicalize

    use rust_embed::RustEmbed;

    #[derive(RustEmbed)]
    #[folder = "src/dbt_macro_assets/"]
    pub struct MacroAssets;
}

pub async fn load_packages(
    arg: &LoadArgs,
    env: &JinjaEnv,
    dbt_profile: &DbtProfile,
    collected_vars: &mut Vec<(String, BTreeMap<String, DbtVars>)>,
    lookup_map: &BTreeMap<String, String>,
    packages_install_path: &Path,
    token: &CancellationToken,
) -> FsResult<Vec<DbtPackage>> {
    // Collect dependency package paths with a flag set to `true`
    // indicating that they are indeed dependencies. This is necessary
    // to differentiate between root project and dependencies later on.
    // 依存関係パッケージのパスを収集し、それらが実際に依存関係であることを示すフラグを 
    // `true` に設定します。これは、後でルートプロジェクトと依存関係を区別するために必要です。
    let mut dirs = if packages_install_path.exists() {
        stdfs::read_dir(packages_install_path)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_type()
                    .map(|ft| ft.is_dir() || ft.is_symlink())
                    .unwrap_or(false)
            })
            .map(|e| (e.path(), true))
            .collect()
    } else {
        vec![]
    };
    // Sort packages to make the output deterministic
    dirs.sort();
    // Add root package to the front of the list
    // `false` indicates that this is a root project
    dirs.insert(0, (arg.io.in_dir.clone(), false));

    collect_packages(
        arg,
        env,
        dbt_profile,
        collected_vars,
        dirs,
        lookup_map,
        token,
    )
    .await
}

pub async fn load_internal_packages(
    arg: &LoadArgs,
    env: &JinjaEnv,
    dbt_profile: &DbtProfile,
    collected_vars: &mut Vec<(String, BTreeMap<String, DbtVars>)>,
    internal_packages_install_path: &Path,
    token: &CancellationToken,
) -> FsResult<Vec<DbtPackage>> {
    let mut dbt_internal_packages_dirs: Vec<(PathBuf, bool)> =
        stdfs::read_dir(internal_packages_install_path)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|ft| ft.is_dir()).unwrap_or(false)) // `true` indicates that this package path is a "dependency", not a root project
            .map(|e| (e.path(), true))
            .collect();
    dbt_internal_packages_dirs.sort();
    collect_packages(
        arg,
        env,
        dbt_profile,
        collected_vars,
        dbt_internal_packages_dirs,
        &BTreeMap::new(),
        token,
    )
    .await
}

/// Load internal packages
pub fn persist_internal_packages(
    internal_packages_install_path: &Path,
    adapter_type: AdapterType,
) -> FsResult<()> {
    // Remove existing folders in the internal_packages_install_path
    // to prevent user from modifying them
    let _ = std::fs::remove_dir_all(internal_packages_install_path);
    // Copy the dbt-adapters and dbt-{adapter_type} to the packages_install_path
    let adapter_package = format!("dbt-{adapter_type}");
    let mut internal_packages = vec!["dbt-adapters", &adapter_package];
    // Some adapters have extra dependencies
    match adapter_type {
        AdapterType::Redshift => internal_packages.push("dbt-postgres"),
        AdapterType::Databricks => internal_packages.push("dbt-spark"),
        _ => {}
    }
    // Copy each macro asset to the packages install path, skipping excluded paths
    for package in internal_packages {
        let mut found = false;
        for asset in assets::MacroAssets::iter() {
            let asset_path = asset.as_ref();
            // Check if this asset belongs to the current package
            if !asset_path.starts_with(package) {
                continue;
            }
            found = true;

            let install_path = internal_packages_install_path.join(asset_path);

            // Create parent directories
            if let Some(parent) = install_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }

            // Copy the asset contents to the install path
            let asset_contents = assets::MacroAssets::get(asset_path).unwrap();
            std::fs::write(install_path, asset_contents.data).unwrap();
        }

        if !found {
            return err!(
                ErrorCode::InvalidConfig,
                "Missing default macro package '{}' for adapter type '{}'",
                package,
                adapter_type
            );
        }
    }
    Ok(())
}

async fn collect_packages(
    arg: &LoadArgs,
    env: &JinjaEnv,
    dbt_profile: &DbtProfile,
    collected_vars: &mut Vec<(String, BTreeMap<String, DbtVars>)>,
    package_paths: Vec<(PathBuf, bool)>,
    lookup_map: &BTreeMap<String, String>,
    token: &CancellationToken,
) -> FsResult<Vec<DbtPackage>> {
    let mut packages = vec![];
    // `is_dependency` Indicates if we are loading a dependency or a root project
    // `is_dependency` は、依存関係またはルートプロジェクトをロードしているかどうかを示します
    for (package_path, is_dependency) in package_paths {
        token.check_cancellation()?;
        if package_path.is_dir() {
            if package_path.join(DBT_PROJECT_YML).exists() {
                let package = load_inner(
                    arg,
                    env,
                    &package_path,
                    dbt_profile,
                    is_dependency,
                    lookup_map,
                    false,
                    collected_vars,
                )
                .await?;
                packages.push(package);
            } else {
                show_warning!(
                    arg.io,
                    fs_err!(
                        ErrorCode::InvalidConfig,
                        "Package {} does not contain a dbt_project.yml file",
                        package_path.file_name().unwrap().to_str().unwrap()
                    )
                );
            }
        }
    }
    Ok(packages)
}
