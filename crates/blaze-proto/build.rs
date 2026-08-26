//! 编译 proto 定义：使用 vendored protoc，无需系统安装。
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // 构建脚本为单线程，设置环境变量无并发风险
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }
    tonic_prost_build::configure().compile_protos(&["proto/blazenet/upload.proto"], &["proto"])?;
    Ok(())
}
