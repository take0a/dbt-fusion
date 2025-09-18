use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use crate::utils::get_original_file_path;
use dbt_schemas::schemas::{
    CommonAttributes, common::DbtChecksum, manifest::DbtOperation, project::DbtProject,
};
use dbt_serde_yaml::Spanned;

pub fn resolve_operations(
    dbt_project: &DbtProject,
    package_base_path: &Path,
    project_root: &Path,
) -> (Vec<Spanned<DbtOperation>>, Vec<Spanned<DbtOperation>>) {
    let mut on_run_start = Vec::new();
    let mut on_run_end = Vec::new();

    for start in dbt_project.on_run_start.iter() {
        let operations: Vec<Spanned<String>> = start.clone().into();
        on_run_start.extend(new_operation(
            "on_run_start",
            &operations,
            dbt_project,
            package_base_path,
            project_root,
        ));
    }

    for end in dbt_project.on_run_end.iter() {
        let operations: Vec<Spanned<String>> = end.clone().into();
        on_run_end.extend(new_operation(
            "on_run_end",
            &operations,
            dbt_project,
            package_base_path,
            project_root,
        ));
    }

    (on_run_start, on_run_end)
}

fn new_operation(
    operation_type: &str,
    operations: &[Spanned<String>],
    dbt_project: &DbtProject,
    package_base_path: &Path,
    project_root: &Path,
) -> Vec<Spanned<DbtOperation>> {
    let project_name = &dbt_project.name;
    // Calculate the original file path for dbt_project.yml
    // dbt_project.yml の元のファイルパスを計算する
    let dbt_project_yml_path = PathBuf::from("dbt_project.yml");
    let original_file_path =
        get_original_file_path(package_base_path, project_root, &dbt_project_yml_path);

    // Map with index
    operations
        .iter()
        .enumerate()
        .map(|(index, operation_sql)| {
            let name = format!("{project_name}-{operation_type}-{index}");
            let unique_id = format!("operation.{project_name}.{name}");
            operation_sql.clone().map(|operation_sql| DbtOperation {
                __common_attr__: CommonAttributes {
                    name: name.clone(),
                    package_name: project_name.to_string(),
                    path: PathBuf::from("hooks").join(&name),
                    original_file_path: original_file_path.clone(),
                    unique_id,
                    fqn: vec![project_name.to_string(), "hooks".to_string(), name],
                    checksum: DbtChecksum::hash(operation_sql.trim().as_bytes()),
                    raw_code: Some(operation_sql.to_string()),
                    language: Some("sql".to_string()),
                    ..Default::default()
                },
                __other__: BTreeMap::new(),
            })
        })
        .collect()
}
