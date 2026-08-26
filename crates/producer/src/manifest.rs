//! 版本清单（.gameindex）二进制格式：生成与解析。
//! 格式见 docs/06-数据存储设计文档.md §3。
use anyhow::{Result, bail};
use std::io::{Cursor, Read};

use crate::chunker::ChunkMeta;

pub const MAGIC: &[u8; 5] = b"BLZGI";
pub const FORMAT_VERSION: u16 = 1;
pub const HASH_LEN: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    pub name: String,
    pub file_hash: [u8; HASH_LEN],
    pub chunks: Vec<ChunkMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GameIndex {
    pub format_version: u16,
    pub files: Vec<FileEntry>,
    /// 整文件 BLAKE3（不含尾部的哈希本身）。
    pub manifest_hash: [u8; HASH_LEN],
}

impl GameIndex {
    /// 由文件条目构建清单，并计算整文件哈希。
    pub fn build(files: Vec<FileEntry>) -> Self {
        let mut index = Self {
            format_version: FORMAT_VERSION,
            files,
            manifest_hash: [0u8; HASH_LEN],
        };
        let encoded = index.encode_body();
        index.manifest_hash = blake3::hash(&encoded).into();
        index
    }

    /// 编码为完整清单字节（含尾部哈希）。
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut body = self.encode_body();
        body.extend_from_slice(&self.manifest_hash);
        Ok(body)
    }

    /// 从字节解析清单；长度不足、magic 错误或尾部哈希不匹配时失败。
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < MAGIC.len() + 2 + 2 + 4 + HASH_LEN {
            bail!("清单长度不足");
        }
        let (body, tail) = bytes.split_at(bytes.len() - HASH_LEN);
        let mut cursor = Cursor::new(body);
        let mut magic = [0u8; MAGIC.len()];
        cursor.read_exact(&mut magic)?;
        if &magic != MAGIC {
            bail!("清单 magic 错误");
        }
        let format_version = read_u16(&mut cursor)?;
        let _flags = read_u16(&mut cursor)?;
        let file_count = read_u32(&mut cursor)? as usize;
        let mut files = Vec::with_capacity(file_count);
        for _ in 0..file_count {
            let name_len = read_u16(&mut cursor)? as usize;
            let mut name_bytes = vec![0u8; name_len];
            cursor.read_exact(&mut name_bytes)?;
            let name =
                String::from_utf8(name_bytes).map_err(|_| anyhow::anyhow!("文件名不是 UTF-8"))?;
            let mut file_hash = [0u8; HASH_LEN];
            cursor.read_exact(&mut file_hash)?;
            let chunk_count = read_u32(&mut cursor)? as usize;
            let mut chunks = Vec::with_capacity(chunk_count);
            for _ in 0..chunk_count {
                let mut hash = [0u8; HASH_LEN];
                cursor.read_exact(&mut hash)?;
                let len = read_u32(&mut cursor)?;
                chunks.push(ChunkMeta { hash, len });
            }
            files.push(FileEntry {
                name,
                file_hash,
                chunks,
            });
        }
        if cursor.position() as usize != body.len() {
            bail!("清单存在多余数据");
        }
        let mut manifest_hash = [0u8; HASH_LEN];
        manifest_hash.copy_from_slice(tail);
        let actual: [u8; HASH_LEN] = blake3::hash(body).into();
        if actual != manifest_hash {
            bail!("清单哈希校验失败");
        }
        Ok(Self {
            format_version,
            files,
            manifest_hash,
        })
    }

    /// 去重后的全部块哈希集合（跨文件共享块只计一次）。
    pub fn chunk_set(&self) -> std::collections::HashSet<[u8; HASH_LEN]> {
        self.files
            .iter()
            .flat_map(|f| f.chunks.iter().map(|c| c.hash))
            .collect()
    }

    fn encode_body(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.format_version.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&(self.files.len() as u32).to_le_bytes());
        for file in &self.files {
            let name = file.name.as_bytes();
            out.extend_from_slice(&(name.len() as u16).to_le_bytes());
            out.extend_from_slice(name);
            out.extend_from_slice(&file.file_hash);
            out.extend_from_slice(&(file.chunks.len() as u32).to_le_bytes());
            for chunk in &file.chunks {
                out.extend_from_slice(&chunk.hash);
                out.extend_from_slice(&chunk.len.to_le_bytes());
            }
        }
        out
    }
}

fn read_u16(cursor: &mut Cursor<&[u8]>) -> Result<u16> {
    let mut buf = [0u8; 2];
    cursor.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, chunks: Vec<(u8, u32)>) -> FileEntry {
        FileEntry {
            name: name.to_string(),
            file_hash: [name.len() as u8; HASH_LEN],
            chunks: chunks
                .into_iter()
                .map(|(h, len)| ChunkMeta {
                    hash: [h; HASH_LEN],
                    len,
                })
                .collect(),
        }
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let index = GameIndex::build(vec![
            entry("a.bin", vec![(1, 10), (2, 20)]),
            entry("目录/b.bin", vec![(3, 30)]),
        ]);
        let bytes = index.encode().unwrap();
        let decoded = GameIndex::decode(&bytes).unwrap();
        assert_eq!(decoded, index);
    }

    #[test]
    fn test_decode_too_short() {
        let err = GameIndex::decode(&[0u8; 10]).unwrap_err();
        assert!(err.to_string().contains("长度不足"));
    }

    #[test]
    fn test_decode_bad_magic() {
        let index = GameIndex::build(vec![]);
        let mut bytes = index.encode().unwrap();
        bytes[0] = b'X';
        let err = GameIndex::decode(&bytes).unwrap_err();
        assert!(err.to_string().contains("magic"));
    }

    #[test]
    fn test_decode_hash_mismatch() {
        let index = GameIndex::build(vec![entry("a.bin", vec![(1, 10)])]);
        let mut bytes = index.encode().unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        let err = GameIndex::decode(&bytes).unwrap_err();
        assert!(err.to_string().contains("哈希校验失败"));
    }

    #[test]
    fn test_decode_truncated_file() {
        let index = GameIndex::build(vec![entry("a.bin", vec![(1, 10)])]);
        let bytes = index.encode().unwrap();
        assert!(GameIndex::decode(&bytes[..bytes.len() - 40]).is_err());
    }

    #[test]
    fn test_chunk_set_dedup() {
        let index = GameIndex::build(vec![
            entry("a.bin", vec![(1, 10), (2, 20)]),
            entry("b.bin", vec![(1, 10), (3, 30)]),
        ]);
        assert_eq!(index.chunk_set().len(), 3);
    }

    #[test]
    fn test_decode_invalid_utf8_name() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.push(0xff);
        bytes.extend_from_slice(&[0u8; HASH_LEN]);
        bytes.extend_from_slice(&0u32.to_le_bytes());
        let mut full = bytes.clone();
        let hash: [u8; HASH_LEN] = blake3::hash(&bytes).into();
        full.extend_from_slice(&hash);
        let err = GameIndex::decode(&full).unwrap_err();
        assert!(err.to_string().contains("UTF-8"));
    }

    #[test]
    fn test_decode_trailing_data() {
        let index = GameIndex::build(vec![entry("a.bin", vec![(1, 10)])]);
        let mut bytes = index.encode().unwrap();
        bytes.extend_from_slice(&[0u8; 4]);
        let err = GameIndex::decode(&bytes).unwrap_err();
        assert!(err.to_string().contains("多余数据"));
    }
}
