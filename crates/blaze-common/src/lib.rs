//! BlazeNet 公共库：协议类型、配置、错误码等共享内容。

/// 返回包名，供日志与错误信息使用。
pub fn package_name() -> &'static str {
    "blazenet"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_package_name() {
        assert_eq!(package_name(), "blazenet");
    }
}
