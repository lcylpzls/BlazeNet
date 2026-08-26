//! BlazeNet 三期 IO 性能基准：对比 std 同步、tokio 异步与 compio（io_uring）顺序读吞吐。
//!
//! 用法：blaze-bench [size_mb] [文件路径]；缺省 256MiB、临时目录。
//! 本工具用于三期 P3.1 评估，输出简体中文性能报告。
use anyhow::{Context, Result};
use std::hint::black_box;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const MIB: u64 = 1024 * 1024;
/// 与块库一致的 1MiB 块与上传大块 8MiB。
const CHUNK_SIZES: [usize; 2] = [MIB as usize, 8 * MIB as usize];

/// 确定性伪随机填充，保证各实现读取内容一致。
fn fill(buf: &mut [u8], seed: &mut u64) {
    for chunk in buf.chunks_mut(8) {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        let value = seed.to_le_bytes();
        for (dst, src) in chunk.iter_mut().zip(value.iter().cycle()) {
            *dst = *src;
        }
    }
}

/// 生成测试文件并返回写入吞吐。
fn create_file(path: &std::path::Path, size: u64) -> Result<(Duration, u64)> {
    let start = Instant::now();
    let mut file = std::fs::File::create(path)
        .with_context(|| format!("创建测试文件失败: {}", path.display()))?;
    let mut buf = vec![0u8; 8 * MIB as usize];
    let mut seed = 0x2026_0827_1234_5678u64;
    let mut written = 0u64;
    while written < size {
        let n = ((size - written) as usize).min(buf.len());
        fill(&mut buf[..n], &mut seed);
        std::io::Write::write_all(&mut file, &buf[..n]).context("写入测试文件失败")?;
        written += n as u64;
    }
    file.sync_all().context("同步测试文件失败")?;
    Ok((start.elapsed(), written))
}

/// std 同步 BufReader 顺序读。
fn bench_std(path: &std::path::Path, chunk: usize) -> Result<(Duration, u64)> {
    let start = Instant::now();
    let file = std::fs::File::open(path)
        .with_context(|| format!("打开测试文件失败: {}", path.display()))?;
    let mut reader = BufReader::with_capacity(chunk, file);
    let mut buf = vec![0u8; chunk];
    let mut total = 0u64;
    let mut sum = 0u64;
    loop {
        let n = reader.read(&mut buf).context("std 顺序读失败")?;
        if n == 0 {
            break;
        }
        total += n as u64;
        sum = sum.wrapping_add(
            buf[..n]
                .iter()
                .fold(0u64, |acc, &b| acc.wrapping_add(b as u64)),
        );
    }
    black_box(sum);
    Ok((start.elapsed(), total))
}

/// tokio 异步顺序读。
async fn bench_tokio(path: &std::path::Path, chunk: usize) -> Result<(Duration, u64)> {
    use tokio::io::AsyncReadExt;
    let start = Instant::now();
    let mut file = tokio::fs::File::open(path)
        .await
        .context("tokio 打开测试文件失败")?;
    let mut buf = vec![0u8; chunk];
    let mut total = 0u64;
    let mut sum = 0u64;
    loop {
        let n = file.read(&mut buf).await.context("tokio 顺序读失败")?;
        if n == 0 {
            break;
        }
        total += n as u64;
        sum = sum.wrapping_add(
            buf[..n]
                .iter()
                .fold(0u64, |acc, &b| acc.wrapping_add(b as u64)),
        );
    }
    black_box(sum);
    Ok((start.elapsed(), total))
}

/// compio（io_uring）按偏移顺序读。
fn bench_compio(path: &std::path::Path, chunk: usize) -> Result<(Duration, u64)> {
    compio::runtime::Runtime::new()
        .context("创建 compio 运行时失败")?
        .block_on(async move {
            use compio::buf::BufResult;
            use compio::io::AsyncReadAtExt;
            let start = Instant::now();
            let file = compio::fs::File::open(path)
                .await
                .context("compio 打开测试文件失败")?;
            let mut buf = vec![0u8; chunk];
            let mut pos = 0u64;
            let mut total = 0u64;
            let mut sum = 0u64;
            loop {
                let BufResult(res, returned) = file.read_exact_at(buf, pos).await;
                buf = returned;
                match res {
                    Ok(()) => {
                        pos += chunk as u64;
                        total += chunk as u64;
                        sum = sum.wrapping_add(
                            buf.iter().fold(0u64, |acc, &b| acc.wrapping_add(b as u64)),
                        );
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e.into()),
                }
            }
            black_box(sum);
            Ok((start.elapsed(), total))
        })
}

/// BLAKE3 内存哈希参考吞吐（与磁盘读无依赖，体现哈希本身速度）。
fn bench_blake3(chunk: usize) -> Result<(Duration, u64)> {
    let mut buf = vec![0u8; chunk];
    let mut seed = 0x9E37_79B9_7F4A_7C15u64;
    fill(&mut buf, &mut seed);
    let start = Instant::now();
    let mut hasher = blake3::Hasher::new();
    for _ in 0..128 {
        hasher.update(&buf);
    }
    let digest = hasher.finalize();
    black_box(digest);
    Ok((start.elapsed(), chunk as u64 * 128))
}

fn mbps(bytes: u64, elapsed: Duration) -> f64 {
    bytes as f64 / elapsed.as_secs_f64() / MIB as f64
}

fn run_case(name: &str, chunk: usize, result: Result<(Duration, u64)>) -> Result<(f64, f64)> {
    let (elapsed, bytes) = result?;
    let rate = mbps(bytes, elapsed);
    println!(
        "{name:<16} {chunk_mib:>5}MiB  {elapsed:>8.3?}s  {rate:>10.1} MiB/s",
        chunk_mib = chunk as u64 / MIB
    );
    Ok((rate, elapsed.as_secs_f64()))
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let size_mb: u64 = args
        .next()
        .map(|v| v.parse().context("size_mb 参数必须是整数"))
        .transpose()?
        .unwrap_or(256);
    let path = args.next().map(PathBuf::from).unwrap_or_else(|| {
        std::env::temp_dir().join(format!("blazenet-bench-{}.dat", std::process::id()))
    });
    let size = size_mb * MIB;
    println!("BlazeNet IO 性能基准（三期 P3.1）");
    println!(
        "平台: {} 内核: {}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    let (write_elapsed, written) = create_file(&path, size)?;
    println!(
        "测试文件: {}（{size_mb} MiB，写入 {:.1} MiB/s）",
        path.display(),
        mbps(written, write_elapsed)
    );
    println!();
    println!("实现            块大小  耗时      吞吐");
    let mut rows = Vec::new();
    for chunk in CHUNK_SIZES {
        let (rate, secs) = run_case("std BufReader", chunk, bench_std(&path, chunk))?;
        rows.push(("std BufReader", chunk, rate, secs));
    }
    for chunk in CHUNK_SIZES {
        let (rate, secs) = run_case(
            "tokio fs",
            chunk,
            tokio::runtime::Runtime::new()
                .context("创建 tokio 运行时失败")?
                .block_on(bench_tokio(&path, chunk)),
        )?;
        rows.push(("tokio fs", chunk, rate, secs));
    }
    for chunk in CHUNK_SIZES {
        let (rate, secs) = run_case("compio io_uring", chunk, bench_compio(&path, chunk))?;
        rows.push(("compio io_uring", chunk, rate, secs));
    }
    for chunk in CHUNK_SIZES {
        let (rate, secs) = run_case("BLAKE3 内存哈希", chunk, bench_blake3(chunk))?;
        rows.push(("BLAKE3 内存哈希", chunk, rate, secs));
    }
    println!();
    println!("汇总（MiB/s）:");
    println!("实现            1MiB 块    8MiB 块");
    for name in [
        "std BufReader",
        "tokio fs",
        "compio io_uring",
        "BLAKE3 内存哈希",
    ] {
        let one = rows
            .iter()
            .find(|r| r.0 == name && r.1 == CHUNK_SIZES[0])
            .map(|r| r.2)
            .unwrap_or(0.0);
        let eight = rows
            .iter()
            .find(|r| r.0 == name && r.1 == CHUNK_SIZES[1])
            .map(|r| r.2)
            .unwrap_or(0.0);
        println!("{name:<16} {one:>10.1}  {eight:>10.1}");
    }
    let _ = std::fs::remove_file(&path);
    Ok(())
}
