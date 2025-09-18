use std::path::Path;

use uuid::Uuid;

/// Emit discrete events during dbt execution.
///
/// There are multiple implementations of this trait, depending on the context.
/// The main one is the `FusionSaEventEmitter`, which is used in the
/// source-available version of dbt Fusion.
/// 
/// dbt 実行中に個別のイベントを発行します。
/// 
/// このトレイトには、コンテキストに応じて複数の実装があります。
/// 主なものは `FusionSaEventEmitter` で、これは dbt Fusion のソースコードが利用可能な
/// バージョンで使用されます。
pub trait DiscreteEventEmitter: Send + Sync {
    fn invocation_start_event(
        &self,
        invocation_id: &Uuid,
        root_project_name: &str,
        profile_path: Option<&Path>,
        command: String,
    );

    // TODO(felipecrv): move more events to this trait
    // so we can use different implementations in different contexts
    // TODO(felipecrv): より多くのイベントをこの特性に移動して、
    // 異なるコンテキストで異なる実装を使用できるようにします。
}
