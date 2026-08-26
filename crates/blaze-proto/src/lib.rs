//! BlazeNet gRPC 协议定义（由 proto 生成，见 proto/blazenet/upload.proto）。
pub mod upload {
    tonic::include_proto!("blazenet.upload");
}

/// Upload 服务名，供客户端与服务端使用。
pub const UPLOAD_SERVICE_NAME: &str = "blazenet.upload.Upload";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_name() {
        assert_eq!(UPLOAD_SERVICE_NAME, "blazenet.upload.Upload");
    }
}
