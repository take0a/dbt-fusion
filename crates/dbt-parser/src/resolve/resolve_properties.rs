use crate::args::ResolveArgs;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::IoArgs;
use dbt_common::io_utils::try_read_yml_to_str;
use dbt_common::{
    ErrorCode, FsResult, constants::PARSING, fs_err, fsinfo, show_error, show_progress,
    show_warning,
};
use dbt_common::{show_package_error, show_strict_error};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::serde::{from_yaml_raw, into_typed_with_jinja};
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_schemas::schemas::properties::{
    DbtPropertiesFileValues, MinimalSchemaValue, MinimalTableValue,
};
use dbt_schemas::schemas::serde::FloatOrString;
use dbt_schemas::state::DbtPackage;
use dbt_serde_yaml::{Span, Verbatim};
use itertools::Itertools;
use minijinja::Value as MinijinjaValue;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct MinimalPropertiesEntry {
    pub name: String,
    pub name_span: Span,
    pub relative_path: PathBuf,
    pub schema_value: dbt_serde_yaml::Value,
    pub table_value: Option<dbt_serde_yaml::Value>,
    pub version_info: Option<VersionInfo>,
    pub duplicate_paths: Vec<PathBuf>,
}

#[derive(Default, Debug)]
pub struct MinimalProperties {
    pub source_tables: BTreeMap<(String, String), MinimalPropertiesEntry>,
    pub models: BTreeMap<String, MinimalPropertiesEntry>,
    pub seeds: BTreeMap<String, MinimalPropertiesEntry>,
    pub snapshots: BTreeMap<String, MinimalPropertiesEntry>,
    pub unit_tests: BTreeMap<String, MinimalPropertiesEntry>,
    pub tests: BTreeMap<String, MinimalPropertiesEntry>,
    pub exposures: BTreeMap<String, MinimalPropertiesEntry>,
    pub saved_queries: BTreeMap<String, MinimalPropertiesEntry>,
    pub groups: BTreeMap<String, MinimalPropertiesEntry>,
}

// impl try extend from MinimalResolvedProperties
#[allow(clippy::cognitive_complexity)]
impl MinimalProperties {
    pub fn extend_from_minimal_properties_file(
        &mut self,
        io_args: &IoArgs,
        other: DbtPropertiesFileValues,
        jinja_env: &JinjaEnv,
        properties_path: &Path,
        base_ctx: &BTreeMap<String, MinijinjaValue>,
    ) -> FsResult<()> {
        // TODO: This is a bit repetetive. Can be shortened!
        // TODO: 少し繰り返しが多いので、短くできます。
        if let Some(models) = other.models {
            // Extend but error on duplicate keys
            // 拡張しますが、重複キーでエラーが発生します
            for model_value in models {
                let model = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    io_args,
                    model_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    &[],
                    dependency_package_name_from_ctx(jinja_env, base_ctx),
                )?;
                for (key, maybe_version_info) in collect_model_version_info(&model).into_iter() {
                    if let Some(existing_model) = self.models.get_mut(&key) {
                        existing_model
                            .duplicate_paths
                            .push(properties_path.to_path_buf());
                    } else {
                        self.models.insert(
                            key,
                            MinimalPropertiesEntry {
                                name: validate_resource_name(&model.name)?,
                                name_span: Span::default(),
                                relative_path: properties_path.to_path_buf(),
                                version_info: maybe_version_info,
                                schema_value: model_value.clone(),
                                table_value: None,
                                duplicate_paths: vec![],
                            },
                        );
                    }
                }
            }
        }
        if let Some(sources) = other.sources {
            for source_value in sources {
                let source = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    io_args,
                    source_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    &[],
                    dependency_package_name_from_ctx(jinja_env, base_ctx),
                )?;

                if let Some(tables) = &*source.tables {
                    // Clone the original source_value to preserve all field spans
                    // and only modify the tables field to null
                    let mut schema_value = source_value.clone();

                    // Set only the tables field to null while preserving all other fields and their spans
                    if let Some(mapping) = schema_value.as_mapping_mut() {
                        mapping.insert(
                            dbt_serde_yaml::Value::string("tables".to_string()),
                            dbt_serde_yaml::Value::null(),
                        );
                    }

                    validate_resource_name(&source.name)?;
                    for table in tables.iter() {
                        let minimum_table_value = into_typed_with_jinja::<MinimalTableValue, _>(
                            io_args,
                            table.clone(),
                            false,
                            jinja_env,
                            base_ctx,
                            &[],
                            dependency_package_name_from_ctx(jinja_env, base_ctx),
                        )?;
                        let key = (
                            source.name.clone(),
                            minimum_table_value.name.clone().into_inner(),
                        );

                        if let Some(existing_entry) = self.source_tables.get_mut(&key) {
                            existing_entry
                                .duplicate_paths
                                .push(properties_path.to_path_buf());

                            show_warning!(
                                io_args,
                                fs_err!(
                                    ErrorCode::SchemaError,
                                    "Duplicate definition for table '{}' in source '{}' found in file '{}'. Using definition from '{}'.",
                                    minimum_table_value.name.clone().into_inner(),
                                    source.name,
                                    properties_path.display(),
                                    existing_entry.relative_path.display()
                                )
                            );
                        } else {
                            self.source_tables.insert(
                                key,
                                MinimalPropertiesEntry {
                                    name: minimum_table_value.name.clone().into_inner(),
                                    name_span: minimum_table_value.name.span().clone(),
                                    relative_path: properties_path.to_path_buf(),
                                    schema_value: schema_value.clone(),
                                    table_value: Some(table.clone()), // Store table separately
                                    version_info: None,
                                    duplicate_paths: vec![],
                                },
                            );
                        }
                    }
                } else {
                    show_warning!(
                        io_args,
                        fs_err!(
                            ErrorCode::SchemaError,
                            "No tables defined for source '{}' in file '{}'.",
                            source.name,
                            properties_path.display()
                        )
                    );
                }
            }
        }
        if let Some(seeds) = other.seeds {
            for seed_value in seeds {
                let seed = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    io_args,
                    seed_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    &[],
                    dependency_package_name_from_ctx(jinja_env, base_ctx),
                )?;
                if let Some(existing_seed) = self.seeds.get_mut(&seed.name) {
                    existing_seed
                        .duplicate_paths
                        .push(properties_path.to_path_buf());
                } else {
                    self.seeds.insert(
                        seed.name.clone(),
                        MinimalPropertiesEntry {
                            name: validate_resource_name(&seed.name)?,
                            name_span: Span::default(),
                            relative_path: properties_path.to_path_buf(),
                            schema_value: seed_value,
                            table_value: None,
                            version_info: None,
                            duplicate_paths: vec![],
                        },
                    );
                }
            }
        }
        if let Some(snapshots) = other.snapshots {
            for snapshot_value in snapshots {
                let snapshot = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    io_args,
                    snapshot_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    &[],
                    dependency_package_name_from_ctx(jinja_env, base_ctx),
                )?;
                if let Some(existing_snapshot) = self.snapshots.get_mut(&snapshot.name) {
                    existing_snapshot
                        .duplicate_paths
                        .push(properties_path.to_path_buf());
                } else {
                    self.snapshots.insert(
                        snapshot.name.clone(),
                        MinimalPropertiesEntry {
                            name: validate_resource_name(&snapshot.name)?,
                            name_span: Span::default(),
                            relative_path: properties_path.to_path_buf(),
                            schema_value: snapshot_value,
                            table_value: None,
                            version_info: None,
                            duplicate_paths: vec![],
                        },
                    );
                }
            }
        }
        if let Some(exposures) = other.exposures {
            for exposure_value in exposures {
                let exposure = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    io_args,
                    exposure_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    &[],
                    dependency_package_name_from_ctx(jinja_env, base_ctx),
                )?;
                self.exposures.insert(
                    exposure.name.clone(),
                    MinimalPropertiesEntry {
                        name: validate_resource_name(&exposure.name)?,
                        name_span: Span::default(),
                        relative_path: properties_path.to_path_buf(),
                        schema_value: exposure_value,
                        table_value: None,
                        version_info: None,
                        duplicate_paths: vec![],
                    },
                );
            }
        }
        if let Some(saved_queries) = other.saved_queries {
            for saved_query_value in saved_queries {
                let saved_query = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    io_args,
                    saved_query_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    &[],
                    dependency_package_name_from_ctx(jinja_env, base_ctx),
                )?;
                if let Some(existing_saved_query) = self.saved_queries.get_mut(&saved_query.name) {
                    existing_saved_query
                        .duplicate_paths
                        .push(properties_path.to_path_buf());
                } else {
                    self.saved_queries.insert(
                        saved_query.name.clone(),
                        MinimalPropertiesEntry {
                            name: validate_resource_name(&saved_query.name)?,
                            name_span: Span::default(),
                            relative_path: properties_path.to_path_buf(),
                            schema_value: saved_query_value,
                            table_value: None,
                            version_info: None,
                            duplicate_paths: vec![],
                        },
                    );
                }
            }
        }
        if let Some(unit_tests) = other.unit_tests {
            for unit_test_value in unit_tests {
                let unit_test = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    io_args,
                    unit_test_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    &[],
                    dependency_package_name_from_ctx(jinja_env, base_ctx),
                )?;
                if let Some(existing_unit_test) = self.unit_tests.get_mut(&unit_test.name) {
                    existing_unit_test
                        .duplicate_paths
                        .push(properties_path.to_path_buf());
                } else {
                    self.unit_tests.insert(
                        unit_test.name.clone(),
                        MinimalPropertiesEntry {
                            name: validate_resource_name(&unit_test.name)?,
                            name_span: Span::default(),
                            relative_path: properties_path.to_path_buf(),
                            schema_value: unit_test_value,
                            table_value: None,
                            version_info: None,
                            duplicate_paths: vec![],
                        },
                    );
                }
            }
        }
        if let Some(tests) = other.tests {
            for test_value in tests {
                let test = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    io_args,
                    test_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    &[],
                    dependency_package_name_from_ctx(jinja_env, base_ctx),
                )?;
                if let Some(existing_test) = self.tests.get_mut(&test.name) {
                    existing_test
                        .duplicate_paths
                        .push(properties_path.to_path_buf());
                } else {
                    self.tests.insert(
                        test.name.clone(),
                        MinimalPropertiesEntry {
                            name: validate_resource_name(&test.name)?,
                            name_span: Span::default(),
                            relative_path: properties_path.to_path_buf(),
                            schema_value: test_value,
                            table_value: None,
                            version_info: None,
                            duplicate_paths: vec![],
                        },
                    );
                }
            }
        }
        if let Some(data_tests) = other.data_tests {
            for test_value in data_tests {
                let test = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    io_args,
                    test_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    &[],
                    dependency_package_name_from_ctx(jinja_env, base_ctx),
                )?;
                if let Some(existing_test) = self.tests.get_mut(&test.name) {
                    existing_test
                        .duplicate_paths
                        .push(properties_path.to_path_buf());
                } else {
                    self.tests.insert(
                        test.name.clone(),
                        MinimalPropertiesEntry {
                            name: validate_resource_name(&test.name)?,
                            name_span: Span::default(),
                            relative_path: properties_path.to_path_buf(),
                            schema_value: test_value,
                            table_value: None,
                            version_info: None,
                            duplicate_paths: vec![],
                        },
                    );
                }
            }
        }
        if let Some(groups) = other.groups {
            for group_value in groups {
                let group = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    io_args,
                    group_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    &[],
                    dependency_package_name_from_ctx(jinja_env, base_ctx),
                )?;
                if let Some(existing_group) = self.groups.get_mut(&group.name) {
                    existing_group
                        .duplicate_paths
                        .push(properties_path.to_path_buf());
                } else {
                    self.groups.insert(
                        group.name.clone(),
                        MinimalPropertiesEntry {
                            name: group.name.clone(),
                            name_span: Span::default(),
                            relative_path: properties_path.to_path_buf(),
                            schema_value: group_value,
                            table_value: None,
                            version_info: None,
                            duplicate_paths: vec![],
                        },
                    );
                }
            }
        }
        Ok(())
    }
}

fn validate_resource_name(name: &str) -> FsResult<String> {
    // Check for the space character for now. This can be extended anytime we deprecate
    // more of special characters like !@#%$":'
    if name.chars().any(|c| matches!(c, ' ')) {
        let err = fs_err!(
            ErrorCode::SchemaError,
            "Resource name '{}' contains forbidden characters",
            name
        );
        Err(err)
    } else {
        Ok(name.to_string())
    }
}

pub fn resolve_minimal_properties(
    arg: &ResolveArgs,
    package: &DbtPackage,
    root_package_name: &str,
    jinja_env: &JinjaEnv,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
    token: &CancellationToken,
) -> FsResult<MinimalProperties> {
    let mut minimal_resolved_properties = MinimalProperties::default();
    for dbt_asset in package.dbt_properties.iter().dedup() {
        token.check_cancellation()?;
        let absolute_path = dbt_asset.base_path.join(&dbt_asset.path);
        show_progress!(
            arg.io,
            fsinfo!(
                PARSING.into(),
                dbt_asset
                    .to_display_path(&arg.io.in_dir)
                    .display()
                    .to_string()
            )
        );

        let dependency_package_name = if package.dbt_project.name != root_package_name {
            Some(package.dbt_project.name.as_str())
        } else {
            None
        };

        let input = try_read_yml_to_str(&absolute_path)?;

        match from_yaml_raw::<DbtPropertiesFileValues>(
            &arg.io,
            &input,
            Some(&absolute_path),
            true,
            dependency_package_name,
        ) {
            Ok(properties_file_values) => {
                minimal_resolved_properties.extend_from_minimal_properties_file(
                    &arg.io,
                    properties_file_values,
                    jinja_env,
                    &dbt_asset.path,
                    base_ctx,
                )?;
            }
            Err(e) => {
                if let Some(package_name) = dependency_package_name
                    && !&arg.io.show_all_deprecations
                {
                    // If we are parsing a dependency package, we use a special macros
                    // that ensures at most one error is shown per package.
                    show_package_error!(&arg.io, package_name);
                } else {
                    show_strict_error!(arg.io, e, dependency_package_name);
                }
                continue; // processing other files
            }
        }
    }
    Ok(minimal_resolved_properties)
}

#[derive(Debug, Clone)]
pub struct VersionInfo {
    pub version: String,
    pub latest_version: String,
    pub versioned_name: String,
    pub version_config: Verbatim<Option<dbt_serde_yaml::Value>>,
    // TODO: Remove this and figure out more efficient way to handle this
    pub all_versions: BTreeMap<String, String>,
}

// Collect and build a properites config for all versions of a model
pub fn collect_model_version_info(
    model: &MinimalSchemaValue,
) -> Vec<(String, Option<VersionInfo>)> {
    if let Some(versions) = &model.versions {
        let mut version_entries = versions
            .iter()
            .map(|v| {
                let version = match &v.v {
                    dbt_serde_yaml::Value::String(s, _) => Some(s.to_string()),
                    dbt_serde_yaml::Value::Number(n, _) => Some(n.to_string()),
                    _ => None,
                }
                .unwrap_or_else(|| {
                    panic!("Version '{:?}' does not meet the required format", v.v);
                });

                let versioned_name = format!("{}_v{}", model.name, version);

                let defined_in = v.__additional_properties__.get("defined_in").and_then(|d| {
                    d.as_str().map(|s| {
                        if s.ends_with(".sql") {
                            s.strip_suffix(".sql").unwrap().to_string()
                        } else {
                            s.to_string()
                        }
                    })
                });

                let version_config = v.config.clone();

                (
                    version,
                    defined_in.unwrap_or(versioned_name),
                    version_config,
                )
            })
            .collect::<Vec<_>>();
        let latest_version = model
            .latest_version
            .clone()
            .map(|v| match v {
                FloatOrString::String(s) => s,
                FloatOrString::Number(n) => n.to_string(),
            })
            .unwrap_or_else(|| {
                // Try parsing as numbers first
                let numeric_versions: Vec<_> = version_entries
                    .iter()
                    .filter_map(|(v, _, _)| v.parse::<f32>().ok())
                    .collect();

                if numeric_versions.len() == version_entries.len() {
                    // If all versions are numeric, use highest number
                    numeric_versions
                        .iter()
                        .reduce(|a, b| if a > b { a } else { b })
                        .map(|n| n.to_string())
                        .expect("Versions should not be empty")
                } else {
                    // Otherwise use lexicographically last
                    version_entries
                        .iter()
                        .map(|(v, _, _)| v)
                        .max()
                        .unwrap()
                        .clone()
                }
            });

        // Find the config for the latest version from existing version entries
        let latest_version_config = version_entries
            .iter()
            .find(|(v, _, _)| v == &latest_version)
            .map(|(_, _, config)| config.clone())
            .unwrap_or_else(|| Verbatim::from(None));

        // Only add the latest version by model.name if it's not already in the list (as in, defined by a defined_in)
        if !version_entries.iter().any(|(_, d, _)| d == &model.name) {
            // how do I get the config for the latest version?
            version_entries.push((
                latest_version.clone(),
                model.name.clone(),
                latest_version_config,
            ));
        }
        version_entries
            .iter()
            .map(|(v, d, config)| {
                (
                    d.clone(),
                    Some(VersionInfo {
                        version: v.clone(),
                        latest_version: latest_version.clone(),
                        versioned_name: d.clone(),
                        all_versions: version_entries
                            .iter()
                            .map(|(v, d, _)| (v.clone(), d.clone()))
                            .collect(),
                        version_config: config.clone(),
                    }),
                )
            })
            .collect()
    } else {
        vec![(model.name.clone(), None)]
    }
}
