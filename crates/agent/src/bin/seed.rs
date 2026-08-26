//! 联调工具：向指定游戏写入测试种子块。
use anyhow::Result;
use std::path::PathBuf;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        anyhow::bail!("用法: seed <数据目录> <game_id> <块数>");
    }
    let data_dir = PathBuf::from(&args[1]);
    let game_id: u64 = args[2].parse()?;
    let chunk_count: usize = args[3].parse()?;
    let mut store = origin::storage::GameStore::open(&data_dir, game_id)?;
    for i in 0..chunk_count {
        let data = format!("blazenet-seed-{game_id}-{i:04}").repeat(64 * 1024);
        let hash: [u8; 32] = blake3::hash(data.as_bytes()).into();
        store.append_chunk(&hash, data.as_bytes())?;
        println!(
            "{i:04}: {}",
            hash.iter().map(|b| format!("{b:02x}")).collect::<String>()
        );
    }
    println!("已写入 {chunk_count} 个种子块（游戏 {game_id}）");
    Ok(())
}
