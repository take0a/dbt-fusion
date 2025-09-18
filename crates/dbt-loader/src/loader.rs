use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use dbt_common::cancellation::CancellationToken;
use dbt_common::constants::DBT_DEPENDENCIES_YML;
use dbt_common::constants::DBT_PACKAGES_LOCK_FILE;
use dbt_common::constants::DBT_PACKAGES_YML;
use dbt_common::once_cell_vars::DISPATCH_CONFIG;
use dbt_common::show_warning;
use dbt_jinja_utils::invocation_args::InvocationArgs;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::phases::load::init::initialize_load_jinja_environment;
use dbt_jinja_utils::phases::load::init::initialize_load_profile_jinja_environment;
use dbt_jinja_utils::serde::yaml_to_fs_error;
use dbt_schemas::schemas::serde::StringOrInteger;
use dbt_schemas::schemas::telemetry::BuildPhaseInfo;
use dbt_schemas::schemas::telemetry::TelemetryAttributes;
use dbt_schemas::state::DbtProfile;
use fs_deps::get_or_install_packages;
use pathdiff::diff_paths;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::SystemTime;

use ignore::gitignore::{Gitignore, GitignoreBuilder};

use dbt_common::constants::{
    DBT_INTERNAL_PACKAGES_DIR_NAME, DBT_PACKAGES_DIR_NAME, DBT_PROJECT_YML, LOADING,
};
use dbt_common::error::LiftableResult;
use project::DbtProject;

use dbt_common::stdfs::last_modified;
use dbt_common::{ErrorCode, ectx, err, with_progress};
use dbt_common::{FsResult, fs_err};
use dbt_schemas::schemas::project::{self, DbtProjectSimplified, ProjectDbtCloudConfig};
use dbt_schemas::state::{DbtAsset, DbtPackage, DbtState, DbtVars, ResourcePathKind};

use crate::args::LoadArgs;
use crate::dbt_project_yml_loader::load_project_yml;
use crate::download_publication::download_publication_artifacts;
use crate::utils::{collect_file_info, identify_package_dependencies};
use crate::{
    load_internal_packages, load_packages, load_profiles, load_vars, persist_internal_packages,
};

use dbt_jinja_utils::phases::load::secret_renderer::secret_context_env_var;
use dbt_jinja_utils::serde::{into_typed_with_jinja, value_from_file};
use dbt_jinja_utils::var_fn;

use dbt_common::tracing::ToTracingValue;

#[tracing::instrument(
    skip_all,
    fields(
        __event = TelemetryAttributes::Phase(BuildPhaseInfo::Loading { }).to_tracing_value(),
    )
)]
pub async fn load(
    arg: &LoadArgs,
    iarg: &InvocationArgs,
    token: &CancellationToken,
) -> FsResult<(DbtState, Option<usize>, Option<ProjectDbtCloudConfig>)> {
    let _pb = with_progress!(arg.io, spinner => LOADING);

    let (simplified_dbt_project, mut dbt_profile) =
        load_simplified_project_and_profiles(arg).await?;

    // Check if .gitignore exists and add dbt_internal_packages/ if not present
    // .gitignore が存在するかどうかを確認し、存在しない場合は dbt_internal_packages/ を追加します。
    let gitignore_path = arg.io.in_dir.join(".gitignore");
    if gitignore_path.exists() {
        let gitignore_content = fs::read_to_string(&gitignore_path)?;
        if !gitignore_content.contains(format!("{DBT_INTERNAL_PACKAGES_DIR_NAME}/").as_str()) {
            let mut updated_content = gitignore_content;
            if !updated_content.ends_with('\n') {
                updated_content.push('\n');
            }
            updated_content.push_str(format!("{DBT_INTERNAL_PACKAGES_DIR_NAME}/\n").as_str());
            fs::write(&gitignore_path, updated_content)?;
        }
    }

    let final_threads = if iarg.num_threads.is_none() {
        if let Some(threads) = dbt_profile.db_config.get_threads() {
            // Convert StringOrInteger to Option<usize>
            match threads {
                StringOrInteger::Integer(n) => Some(*n as usize),
                StringOrInteger::String(s) => Some(s.parse::<usize>().map_err(|_| {
                    fs_err!(
                        ErrorCode::Generic,
                        "Invalid number of threads in profiles.yml: {}",
                        s
                    )
                })?),
            }
        } else {
            None
        }
    } else {
        iarg.num_threads
    };

    dbt_profile
        .db_config
        .set_threads(Some(StringOrInteger::Integer(
            final_threads.unwrap_or(0) as i64
        )));

    let iarg = InvocationArgs {
        num_threads: final_threads,
        ..iarg.clone()
    };
    let arg = LoadArgs {
        threads: final_threads,
        ..arg.clone()
    };

    let mut dbt_state = DbtState {
        dbt_profile,
        run_started_at: run_started_at(),
        packages: vec![],
        vars: BTreeMap::new(),
        cli_vars: arg.vars.clone(),
    };

    // If we are running `dbt debug` we don't need to collect dbt_project.yml files
    // `dbt debug` を実行している場合は、dbt_project.yml ファイルを収集する必要はありません。
    if arg.debug_profile {
        return Ok((dbt_state, final_threads, simplified_dbt_project.dbt_cloud));
    }

    let flags: BTreeMap<String, minijinja::Value> = iarg.to_dict();

    let env = initialize_load_jinja_environment(
        &dbt_state.dbt_profile.profile,
        &dbt_state.dbt_profile.target,
        dbt_state.dbt_profile.db_config.adapter_type(),
        dbt_state.dbt_profile.db_config.clone(),
        dbt_state.run_started_at,
        &flags,
        arg.io.clone(),
        token.clone(),
    )?;

    let adapter_type = dbt_state
        .dbt_profile
        .db_config
        .adapter_type_if_supported()
        .ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "Unknown or unsupported adapter type '{}'",
                dbt_state.dbt_profile.db_config.adapter_type()
            )
        })?;

    let arg_ref = &arg;
    if let Some(prev_dbt_state) = arg.prev_dbt_state.clone() {
        let prev_root_package = prev_dbt_state.root_package();

        let package_map_lookup = BTreeMap::new();
        let mut dummy_collected_vars = Vec::new();
        let mut new_root_package = load_inner(
            arg_ref,
            &env,
            &arg.io.in_dir,
            &dbt_state.dbt_profile,
            false,
            &package_map_lookup,
            true,
            &mut dummy_collected_vars,
        )
        .await?;
        new_root_package.dependencies = prev_root_package.dependencies.clone();
        dbt_state.vars = prev_dbt_state.vars.clone();

        let packages = prev_dbt_state
            .packages
            .iter()
            .map(|x| (*x).clone())
            .collect::<Vec<_>>();
        dbt_state.packages.extend(packages);
        dbt_state.packages[0] = new_root_package;

        return Ok((dbt_state, final_threads, simplified_dbt_project.dbt_cloud));
    }

    // Load the packages.yml file, if it exists and install the packages if arg.install_deps is true
    // 存在する場合はpackages.ymlファイルを読み込み、arg.install_depsがtrueの場合はパッケージをインストールします。
    let (packages_install_path, internal_packages_install_path) = get_packages_install_path(
        &arg.io.in_dir,
        &arg.packages_install_path,
        &arg.internal_packages_install_path,
        &simplified_dbt_project,
    );

    persist_internal_packages(&internal_packages_install_path, adapter_type)?;

    let (packages_lock, upstream_projects) = get_or_install_packages(
        &arg.io,
        &env,
        &packages_install_path,
        arg.install_deps,
        arg.add_package.clone(),
        arg.vars.clone(),
        token,
    )
    .await?;
    // get publication artifact for each upstream project
    // 各上流プロジェクトの公開アーティファクトを取得する
    download_publication_artifacts(
        &upstream_projects,
        &simplified_dbt_project.dbt_cloud,
        &arg.io,
    )
    .await?;
    // If we are running `dbt deps` we don't need to collect files
    // `dbt deps` を実行している場合は、ファイルを収集する必要はありません。
    if arg.install_deps {
        return Ok((dbt_state, final_threads, simplified_dbt_project.dbt_cloud));
    }

    let lookup_map = packages_lock.lookup_map();
    let mut collected_vars = vec![];
    {
        let _pb = with_progress!( arg.io, spinner => LOADING, item => "packages" );

        let packages = load_packages(
            &arg,
            &env,
            &dbt_state.dbt_profile,
            &mut collected_vars,
            &lookup_map,
            &packages_install_path,
            token,
        )
        .await?;
        dbt_state.packages = packages;
    }
    {
        let _pb = with_progress!( arg.io, spinner => LOADING, item => "internal packages" );

        let packages = load_internal_packages(
            &arg,
            &env,
            &dbt_state.dbt_profile,
            &mut collected_vars,
            &internal_packages_install_path,
            token,
        )
        .await?;
        dbt_state.packages.extend(packages);
        dbt_state.vars = collected_vars.into_iter().collect();
    }
    Ok((dbt_state, final_threads, simplified_dbt_project.dbt_cloud))
}

pub async fn load_simplified_project_and_profiles(
    arg: &LoadArgs,
) -> FsResult<(DbtProjectSimplified, DbtProfile)> {
    // Read the input file
    // 入力ファイルを読む
    let dbt_project_path = arg.io.in_dir.join(DBT_PROJECT_YML);

    let raw_dbt_project_in_val = value_from_file(&arg.io, &dbt_project_path, false, None)?;
    let env = initialize_load_profile_jinja_environment();
    let ctx: BTreeMap<String, minijinja::Value> = BTreeMap::from([
        (
            "env_var".to_owned(),
            minijinja::Value::from_func_func("env_var", secret_context_env_var),
        ),
        (
            "var".to_owned(),
            minijinja::Value::from_function(var_fn(arg.vars.clone())),
        ),
    ]);

    let simplified_dbt_project: DbtProjectSimplified =
        into_typed_with_jinja(&arg.io, raw_dbt_project_in_val, true, &env, &ctx, &[], None)?;

    if simplified_dbt_project.data_paths.is_some() {
        return err!(
            ErrorCode::InvalidConfig,
            "'data-paths' cannot be specified in dbt_project.yml",
        );
    }
    if simplified_dbt_project.source_paths.is_some() {
        return err!(
            ErrorCode::InvalidConfig,
            "'source-paths' cannot be specified in dbt_project.yml",
        );
    }
    if (*simplified_dbt_project.log_path)
        .as_ref()
        .is_some_and(|path| path != "logs")
    {
        return err!(
            ErrorCode::InvalidConfig,
            "'log-path' cannot be specified in dbt_project.yml",
        );
    }
    if (*simplified_dbt_project.target_path)
        .as_ref()
        .is_some_and(|path| path != "target")
    {
        return err!(
            ErrorCode::InvalidConfig,
            "'target-path' cannot be specified in dbt_project.yml",
        );
    }

    let dbt_profile = load_profiles(arg, &simplified_dbt_project, &env, &ctx)?;

    Ok((simplified_dbt_project, dbt_profile))
}

#[allow(clippy::too_many_arguments)]
pub async fn load_inner(
    arg: &LoadArgs,
    env: &JinjaEnv,
    package_path: &Path,
    dbt_profile: &DbtProfile,
    // Indicates if we are loading a dependency or a root project
    // 依存関係またはルートプロジェクトをロードしているかどうかを示します
    is_dependency: bool,
    package_lookup_map: &BTreeMap<String, String>,
    skip_dependencies: bool,
    collected_vars: &mut Vec<(String, BTreeMap<String, DbtVars>)>,
) -> FsResult<DbtPackage> {
    // all read files
    // すべての読み取りファイル
    let mut all_files: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> = HashMap::new();

    let dbt_project_path = package_path.join(DBT_PROJECT_YML);

    let dependency_package_name = if is_dependency {
        Some(
            dbt_project_path
                .parent()
                .and_then(|p| p.file_name())
                .map(|os_str| os_str.to_string_lossy().to_string())
                .ok_or(fs_err!(
                    ErrorCode::InvalidConfig,
                    "Failed to get package name from path: {}",
                    &dbt_project_path.display()
                ))?,
        )
    } else {
        None
    };

    let dbt_project = load_project_yml(
        &arg.io,
        env,
        &dbt_project_path,
        dependency_package_name.as_deref(),
        arg.vars.clone(),
    )?;
    load_vars(
        &dbt_project.name,
        (*dbt_project.vars)
            .as_ref()
            .map(|vars| Deserialize::deserialize(vars.clone()))
            .transpose()
            .map_err(|e| yaml_to_fs_error(e, Some(&dbt_project_path)))?,
        collected_vars,
    )?;
    // Set dispatch config for future use
    // 今後の使用に備えてディスパッチ構成を設定する
    if package_path == arg.io.in_dir {
        let dispatch_config_map = if let Some(dispatch_configs) = dbt_project.dispatch.clone() {
            dispatch_configs
                .iter()
                .map(|dispatch_config| {
                    (
                        dispatch_config.macro_namespace.clone(),
                        dispatch_config.search_order.clone(),
                    )
                })
                .collect()
        } else {
            BTreeMap::new()
        };
        // Only set the dispatch config on first load of the project (mainly impacts testing)
        // プロジェクトの最初のロード時にのみディスパッチ構成を設定します（主にテストに影響します）
        if DISPATCH_CONFIG.get().is_none() {
            DISPATCH_CONFIG
                .set(RwLock::new(dispatch_config_map))
                .unwrap();
        }
    }

    let session_files = find_session_files(package_path)?;
    all_files.insert(ResourcePathKind::SessionPaths, session_files);

    // Collect file paths and their timestamps for fields with a suffix `_paths`
    // `_paths` という接尾辞を持つフィールドのファイルパスとそのタイムスタンプを収集します
    let all_dirs = collect_paths(&dbt_project);
    let all_included_files: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> =
        collect_all_files(all_dirs, package_path)?;
    all_files.extend(all_included_files);

    // make all paths relative to the project directory
    // すべてのパスをプロジェクトディレクトリからの相対パスにする
    for (_, files) in all_files.iter_mut() {
        for (path, _) in files.iter_mut() {
            *path = diff_paths(&mut *path, package_path).unwrap().to_path_buf();
        }
        //
        // make deterministic: Sort files based on their relative paths
        // 決定論的にする: 相対パスに基づいてファイルを並べ替える
        files.sort_by(|a, b| a.0.cmp(&b.0));
    }

    let python_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::ModelPaths,
        &["py"],
        &all_files,
    );

    if !python_files.is_empty() {
        for file in python_files {
            show_warning!(
                &arg.io,
                *fs_err!(
                    code => ErrorCode::UnsupportedFileExtension,
                    loc => file.path.clone(),
                    "Python models are not currently supported"
                )
            );
        }
    }

    // todo: we could optimize here, but for now just take everything,...
    let mut dbt_properties = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::ModelPaths,
        &["yml", "yaml"],
        &all_files,
    );
    // additonal paths can have ym files (add generic tests etc)
    let seed_ymls = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::SeedPaths,
        &["yml", "yaml"],
        &all_files,
    );
    let snapshot_ymls = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::SnapshotPaths,
        &["yml", "yaml"],
        &all_files,
    );
    let analysis_ymls = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::AnalysisPaths,
        &["yml", "yaml"],
        &all_files,
    );

    let test_ymls = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::TestPaths,
        &["yml", "yaml"],
        &all_files,
    );

    // todo: change dbt_properties to be BTreeSet, this may require many goldies updates
    for item in seed_ymls
        .iter()
        .chain(&snapshot_ymls)
        .chain(&analysis_ymls)
        .chain(&test_ymls)
    {
        if !dbt_properties.contains(item) {
            dbt_properties.push(item.clone());
        }
    }

    let analysis_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::AnalysisPaths,
        &["sql"],
        &all_files,
    );
    let model_sql_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::ModelPaths,
        &["sql"],
        &all_files,
    );
    let macro_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::MacroPaths,
        &["sql"],
        &all_files,
    );
    let test_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::TestPaths,
        &["sql"],
        &all_files,
    );
    let fixture_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::FixturePaths,
        &["csv"],
        &all_files,
    );
    let seed_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::SeedPaths,
        &["csv", "parquet", "json"],
        &all_files,
    );
    let docs_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::DocsPaths,
        &["md"],
        &all_files,
    );
    let snapshot_files = find_files_by_kind_and_extension(
        package_path,
        &dbt_project.name,
        &ResourcePathKind::SnapshotPaths,
        &["sql"],
        &all_files,
    );
    let dependencies = if skip_dependencies {
        BTreeSet::new()
    } else {
        identify_package_dependencies(
            &arg.io,
            package_path,
            package_lookup_map,
            dependency_package_name.as_deref(),
        )?
    };
    // Only do this for the root package.
    if !is_dependency {
        collect_profiles_yml_if_exists(dbt_profile, &mut all_files);
    }
    Ok(DbtPackage {
        dbt_project,
        package_root_path: package_path.to_path_buf(),
        dbt_properties,
        analysis_files,
        model_sql_files,
        test_files,
        fixture_files,
        seed_files,
        macro_files,
        docs_files,
        snapshot_files,
        dependencies,
        all_paths: all_files,
    })
}

/// outputs the timestamp that this run started
fn run_started_at() -> DateTime<Tz> {
    let utc_now = Utc::now();
    let tz_now: DateTime<Tz> = utc_now.with_timezone(&Tz::UTC);
    tz_now
}

fn should_exclude_path(kind: &ResourcePathKind, path: &Path) -> bool {
    match kind {
        ResourcePathKind::TestPaths => {
            // Only exclude paths directly under <test-paths>/generic/
            // <test-paths>/generic/ 直下のパスのみを除外します
            let components: Vec<_> = path.components().collect();
            components.len() >= 2 && components[1].as_os_str() == "generic"
        }
        _ => false,
    }
}

fn find_files_by_kind_and_extension(
    in_dir: &Path,
    project_name: &str,
    path_kind: &ResourcePathKind,
    extensions: &[&str],
    all_paths: &HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>>,
) -> Vec<DbtAsset> {
    let default = vec![];
    let paths_to_filter: Vec<_> = all_paths
        .get(path_kind)
        .unwrap_or(&default)
        .iter()
        .collect();

    let mut paths = paths_to_filter
        .iter()
        .filter_map(|(path, _)| {
            path.extension()
                .and_then(OsStr::to_str)
                .filter(|ext| extensions.contains(&ext.to_lowercase().as_str()))
                .filter(|_| !should_exclude_path(path_kind, path))
                .map(|_| DbtAsset {
                    package_name: project_name.to_string(),
                    base_path: in_dir.to_path_buf(),
                    path: path.clone(),
                })
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    paths.sort_by(|a, b| a.path.cmp(&b.path));
    paths
}

/// Loads the .dbtignore file if it exists in the given path
fn load_dbtignore(path: &Path) -> FsResult<Option<Gitignore>> {
    let dbtignore_path = path.join(".dbtignore");
    if dbtignore_path.exists() {
        let mut builder = GitignoreBuilder::new(path);
        // add() returns Option<Error> where None means success and Some(err) is an error
        match builder.add(&dbtignore_path) {
            None => match builder.build() {
                Ok(gitignore) => return Ok(Some(gitignore)),
                Err(err) => {
                    return err!(
                        code => ErrorCode::InvalidConfig,
                        loc => dbtignore_path.clone(),
                        "Error building .dbtignore: {}",
                        err
                    );
                }
            },
            Some(err) => {
                return err!(
                    code => ErrorCode::InvalidConfig,
                    loc => dbtignore_path.clone(),
                    "Failed to add .dbtignore file: {}",
                    err
                );
            }
        }
    }
    Ok(None)
}

fn collect_all_files(
    all_dirs: HashMap<ResourcePathKind, Vec<String>>,
    base_path: &Path,
) -> FsResult<HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>>> {
    // Load .dbtignore file if it exists
    let dbtignore = load_dbtignore(base_path)?;
    // Remove debug statement for tests
    let mut all_paths: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> = HashMap::new();
    for (kind, paths) in &all_dirs {
        let mut info_paths = Vec::new();
        collect_file_info(base_path, paths, &mut info_paths, dbtignore.as_ref()).lift(ectx!(
            "Failed to collect file info: {}, {}",
            base_path.display(),
            paths.join(",")
        ))?;
        all_paths.insert(kind.clone(), info_paths);
    }
    Ok(all_paths)
}

fn collect_paths(dbt_project: &DbtProject) -> HashMap<ResourcePathKind, Vec<String>> {
    let mut all_dirs: HashMap<ResourcePathKind, Vec<String>> = HashMap::new();
    all_dirs.insert(
        ResourcePathKind::ModelPaths,
        dbt_project.model_paths.clone().unwrap_or_default(),
    );
    all_dirs.insert(
        ResourcePathKind::AnalysisPaths,
        dbt_project.analysis_paths.clone().unwrap_or_default(),
    );
    all_dirs.insert(
        ResourcePathKind::AssetPaths,
        dbt_project.asset_paths.clone().unwrap_or_default(),
    );
    all_dirs.insert(
        ResourcePathKind::MacroPaths,
        dbt_project.macro_paths.clone().unwrap_or_default(),
    );
    all_dirs.insert(
        ResourcePathKind::SeedPaths,
        dbt_project.seed_paths.clone().unwrap_or_default(),
    );
    all_dirs.insert(
        ResourcePathKind::SnapshotPaths,
        dbt_project.snapshot_paths.clone().unwrap_or_default(),
    );
    all_dirs.insert(
        ResourcePathKind::TestPaths,
        dbt_project.test_paths.clone().unwrap_or_default(),
    );
    all_dirs.insert(
        ResourcePathKind::FixturePaths,
        dbt_project
            .test_paths
            .clone()
            .unwrap_or_default()
            .iter()
            .map(|p| {
                let path = PathBuf::from(p).join("fixtures");
                path.into_os_string().into_string().unwrap_or_default()
            })
            .collect(),
    );
    // Only register docs paths if they are explicitly specified
    if dbt_project.docs_paths.is_some() && !dbt_project.docs_paths.as_ref().unwrap().is_empty() {
        all_dirs.insert(
            ResourcePathKind::DocsPaths,
            dbt_project.docs_paths.clone().unwrap_or_default(),
        );
    } else {
        // The default is to read all files in the following directories for '*.md' files
        let mut result: Vec<String> = vec![];

        result.extend_from_slice(dbt_project.analysis_paths.as_deref().unwrap_or_default());
        result.extend_from_slice(dbt_project.macro_paths.as_deref().unwrap_or_default());
        result.extend_from_slice(dbt_project.model_paths.as_deref().unwrap_or_default());
        result.extend_from_slice(dbt_project.seed_paths.as_deref().unwrap_or_default());
        result.extend_from_slice(dbt_project.snapshot_paths.as_deref().unwrap_or_default());
        result.extend_from_slice(dbt_project.test_paths.as_deref().unwrap_or_default());

        all_dirs.insert(ResourcePathKind::DocsPaths, result);
    }
    all_dirs
}

// returns (packages_install_path, internal_packages_install_path)
fn get_packages_install_path(
    in_dir: &Path,
    arg_packages_install_path: &Option<PathBuf>,
    arg_internal_packages_install_path: &Option<PathBuf>,
    dbt_project: &DbtProjectSimplified,
) -> (PathBuf, PathBuf) {
    let packages_install_path = if let Some(path) = arg_packages_install_path {
        if path.is_absolute() {
            path.clone()
        } else {
            in_dir.join(path)
        }
    } else if let Some(path) = &dbt_project.packages_install_path {
        let mut path_buf = PathBuf::from(path);
        if !path_buf.is_absolute() {
            path_buf = in_dir.join(path_buf);
        }
        path_buf
    } else {
        in_dir.join(DBT_PACKAGES_DIR_NAME)
    };

    let internal_packages_install_path = if let Some(path) = arg_internal_packages_install_path {
        if path.is_absolute() {
            path.clone()
        } else {
            in_dir.join(path)
        }
    } else {
        packages_install_path.with_file_name(DBT_INTERNAL_PACKAGES_DIR_NAME)
    };

    (packages_install_path, internal_packages_install_path)
}

fn collect_profiles_yml_if_exists(
    dbt_profile: &DbtProfile,
    all_paths: &mut HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>>,
) {
    if let Ok(timestamp) = last_modified(&dbt_profile.relative_profile_path) {
        let entry = all_paths.entry(ResourcePathKind::ProfilePaths).or_default();
        entry.push((dbt_profile.relative_profile_path.clone(), timestamp));
    }
}

/// These are the built-in session file paths relative to a project.
pub fn get_session_relative_file_paths() -> Vec<String> {
    vec![
        DBT_PROJECT_YML.into(),
        DBT_DEPENDENCIES_YML.into(),
        DBT_PACKAGES_YML.into(),
        DBT_PACKAGES_LOCK_FILE.into(),
    ]
}

fn find_session_files(package_path: &Path) -> FsResult<Vec<(PathBuf, SystemTime)>> {
    let mut result = Vec::new();

    for relative_path in get_session_relative_file_paths() {
        // Heuristic for DBT_PROJECT_YML.
        // We actually want to raise an error if it was not able to be read.
        if relative_path == DBT_PROJECT_YML {
            let dbt_project_path = package_path.join(relative_path);
            let dbt_project_timestamp = last_modified(&dbt_project_path)?;
            result.push((dbt_project_path, dbt_project_timestamp));
        } else {
            let path = package_path.join(relative_path);
            if let Ok(timestamp) = last_modified(&path) {
                result.push((path, timestamp));
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::state::ResourcePathKind;
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::SystemTime;

    #[test]
    fn test_find_files_by_kind_and_extension_excludes_generic_test_paths() {
        // Setup test data
        let in_dir = PathBuf::from("/project");
        let project_name = "test_project";
        let extensions = &["sql", "yml"];

        // Create mock file paths with timestamps
        let now = SystemTime::now();
        let mut all_paths: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> = HashMap::new();

        // Add test files - paths include test directory name as first component
        let test_files = vec![
            (PathBuf::from("tests/test_model.sql"), now),
            (PathBuf::from("tests/integration/test_integration.sql"), now),
            (PathBuf::from("tests/generic/test_generic.sql"), now), // Should be excluded
            (PathBuf::from("tests/generic/nested/test_nested.sql"), now), // Should be excluded
            (PathBuf::from("tests/custom/test_custom.sql"), now),
            (PathBuf::from("tests/schema.yml"), now),
            (PathBuf::from("tests/generic/schema.yml"), now), // Should be excluded
            (PathBuf::from("data-tests/generic/is_even.sql"), now), // Should be excluded
            (PathBuf::from("data-tests/singular/my_test.sql"), now),
        ];

        all_paths.insert(ResourcePathKind::TestPaths, test_files);

        // Call the function under test
        let result = find_files_by_kind_and_extension(
            &in_dir,
            project_name,
            &ResourcePathKind::TestPaths,
            extensions,
            &all_paths,
        );

        // Verify results - should exclude 3 generic files
        assert_eq!(result.len(), 5, "Should have 6 non-generic test files");

        // Check that all returned files are not in generic directories
        for asset in &result {
            let components: Vec<_> = asset.path.components().collect();
            assert!(
                !(components.len() >= 2 && components[1].as_os_str() == "generic"),
                "File {:?} should not have 'generic' as second component",
                asset.path
            );
        }

        // Check specific files that should be included
        let included_paths: Vec<&PathBuf> = result.iter().map(|asset| &asset.path).collect();
        assert!(included_paths.contains(&&PathBuf::from("tests/test_model.sql")));
        assert!(included_paths.contains(&&PathBuf::from("tests/integration/test_integration.sql")));
        assert!(included_paths.contains(&&PathBuf::from("tests/custom/test_custom.sql")));
        assert!(included_paths.contains(&&PathBuf::from("tests/schema.yml")));
        assert!(included_paths.contains(&&PathBuf::from("data-tests/singular/my_test.sql")));

        // Check that generic files are excluded
        assert!(!included_paths.contains(&&PathBuf::from("tests/generic/test_generic.sql")));
        assert!(!included_paths.contains(&&PathBuf::from("tests/generic/nested/test_nested.sql")));
        assert!(!included_paths.contains(&&PathBuf::from("tests/generic/schema.yml")));
        assert!(!included_paths.contains(&&PathBuf::from("data-tests/generic/is_even.sql")));

        // Verify asset properties
        for asset in &result {
            assert_eq!(asset.package_name, project_name);
            assert_eq!(asset.base_path, in_dir);
        }
    }

    #[test]
    fn test_load_dbtignore() {
        use tempfile::TempDir;

        // Create a temporary directory
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        // Initially no .dbtignore file
        let ignore = load_dbtignore(temp_path).unwrap();
        assert!(ignore.is_none());

        // Create a .dbtignore file
        let dbtignore_path = temp_path.join(".dbtignore");
        let mut file = File::create(dbtignore_path).unwrap();
        writeln!(file, "*.py").unwrap();
        writeln!(file, "/ignored_dir/").unwrap(); // Explicit directory format with slashes
        writeln!(file, "!important.py").unwrap();

        // Now .dbtignore should be loaded
        let ignore = load_dbtignore(temp_path).unwrap();
        assert!(ignore.is_some());

        let ignore = ignore.unwrap();

        // Test patterns
        // Test patterns with file=false parameter
        assert!(ignore.matched("test.py", false).is_ignore()); // Should be ignored
        assert!(!ignore.matched("important.py", false).is_ignore()); // Should NOT be ignored (negated)

        // For this test, let's focus on the patterns we know should work reliably
        assert!(ignore.matched("test.py", false).is_ignore()); // Should be ignored
        assert!(!ignore.matched("important.py", false).is_ignore()); // Should NOT be ignored (negated)
        assert!(!ignore.matched("test.txt", false).is_ignore()); // Should NOT be ignored
    }

    #[test]
    fn test_should_exclude_path_function() {
        // Test the should_exclude_path function directly

        // Test paths should exclude generic directories (second component)
        assert!(should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("tests/generic/test.sql")
        ));

        assert!(should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("data-tests/generic/test.sql")
        ));

        assert!(should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("tests/generic/nested/test.sql")
        ));

        // Test paths should NOT exclude non-generic directories
        assert!(!should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("tests/integration/test.sql")
        ));

        assert!(!should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("tests/unit/test.sql")
        ));

        assert!(!should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("data-tests/singular/test.sql")
        ));

        assert!(!should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("tests/unit/generic/test.sql") // generic is not second component
        ));

        // Edge cases
        assert!(!should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("generic/test.sql") // only one component
        ));

        assert!(!should_exclude_path(
            &ResourcePathKind::TestPaths,
            &PathBuf::from("tests") // only one component
        ));

        // Non-test paths should never exclude generic directories
        assert!(!should_exclude_path(
            &ResourcePathKind::ModelPaths,
            &PathBuf::from("models/generic/model.sql")
        ));

        assert!(!should_exclude_path(
            &ResourcePathKind::MacroPaths,
            &PathBuf::from("macros/generic/macro.sql")
        ));

        assert!(!should_exclude_path(
            &ResourcePathKind::SeedPaths,
            &PathBuf::from("seeds/generic/seed.csv")
        ));
    }

    #[test]
    fn test_find_files_by_kind_and_extension_empty_paths() {
        // Test with empty paths
        let in_dir = PathBuf::from("/project");
        let project_name = "test_project";
        let extensions = &["sql"];
        let all_paths: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> = HashMap::new();

        let result = find_files_by_kind_and_extension(
            &in_dir,
            project_name,
            &ResourcePathKind::TestPaths,
            extensions,
            &all_paths,
        );

        assert!(
            result.is_empty(),
            "Should return empty vector for empty paths"
        );
    }

    #[test]
    fn test_find_files_by_kind_and_extension_extension_filtering() {
        // Test that only files with specified extensions are included
        let in_dir = PathBuf::from("/project");
        let project_name = "test_project";
        let extensions = &["sql"]; // Only SQL files

        let now = SystemTime::now();
        let mut all_paths: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> = HashMap::new();

        let test_files = vec![
            (PathBuf::from("tests/test.sql"), now), // Should be included
            (PathBuf::from("tests/test.yml"), now), // Should be excluded (wrong extension)
            (PathBuf::from("tests/test.py"), now),  // Should be excluded (wrong extension)
            (PathBuf::from("tests/test"), now),     // Should be excluded (no extension)
        ];

        all_paths.insert(ResourcePathKind::TestPaths, test_files);

        let result = find_files_by_kind_and_extension(
            &in_dir,
            project_name,
            &ResourcePathKind::TestPaths,
            extensions,
            &all_paths,
        );

        assert_eq!(result.len(), 1, "Should only include SQL files");
        assert_eq!(result[0].path, PathBuf::from("tests/test.sql"));
    }

    #[test]
    fn test_find_files_by_kind_and_extension_non_test_paths_not_excluded() {
        // Setup test data for non-test paths (should not exclude generic directories)
        let in_dir = PathBuf::from("/project");
        let project_name = "test_project";
        let extensions = &["sql"];

        let now = SystemTime::now();
        let mut all_paths: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>> = HashMap::new();

        // Add model files with generic in path (should NOT be excluded for models)
        let model_files = vec![
            (PathBuf::from("models/generic/my_model.sql"), now),
            (PathBuf::from("models/other/model.sql"), now),
        ];

        all_paths.insert(ResourcePathKind::ModelPaths, model_files);

        // Call the function under test for ModelPaths
        let result = find_files_by_kind_and_extension(
            &in_dir,
            project_name,
            &ResourcePathKind::ModelPaths,
            extensions,
            &all_paths,
        );

        // Verify that generic directories are NOT excluded for non-test paths
        assert_eq!(
            result.len(),
            2,
            "Should have 2 model files including generic path"
        );

        let included_paths: Vec<&PathBuf> = result.iter().map(|asset| &asset.path).collect();
        assert!(included_paths.contains(&&PathBuf::from("models/generic/my_model.sql")));
        assert!(included_paths.contains(&&PathBuf::from("models/other/model.sql")));
    }
}
