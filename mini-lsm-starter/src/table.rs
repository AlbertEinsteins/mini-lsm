// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, BytesMut};
pub use iterator::SsTableIterator;

use crate::block::{self, Block};
use crate::key::{Key, KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        buf.extend_from_slice(&(block_meta.len() as u16).to_be_bytes());
        for meta in block_meta {
            let mut byte_buf = BytesMut::new();
            byte_buf.put_u16(meta.offset as u16);
            byte_buf.put_u16(meta.first_key.len() as u16);
            byte_buf.put(meta.first_key.raw_ref());
            byte_buf.put_u16(meta.last_key.len() as u16);
            byte_buf.put(meta.last_key.raw_ref());
            buf.extend_from_slice(&byte_buf);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut buf = buf;
        let mut block_metas = Vec::new();
        let num_metas = buf.get_u16();

        for idx in 0..num_metas {
            let offset = buf.get_u16() as usize;

            let first_key_len = buf.get_u16() as usize;
            let first_key = {
                let bytes = buf.copy_to_bytes(first_key_len);
                KeyBytes::from_bytes(bytes)
            };
            let last_key_len = buf.get_u16() as usize;
            let last_key = {
                let bytes = buf.copy_to_bytes(last_key_len);
                KeyBytes::from_bytes(bytes)
            };
            block_metas.push(BlockMeta{
                offset,
                first_key,
                last_key
            })
        }
        block_metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    fn read_exact_at(file: &mut File, buf: &mut [u8], offset: u64) -> Result<()> {
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(buf)?;
        Ok(())
    }

    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        // use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        let mut file = self.0.as_ref().unwrap().try_clone()?;
        let _ = Self::read_exact_at(&mut file, &mut data, offset);
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        unimplemented!()
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        assert!(block_idx < self.block_meta.len(), 
            "block idx {} is greater than the total number", block_idx);

        let meta = &self.block_meta[block_idx];        
        let off = meta.offset as u64;
        let block_len_bytes = self.file.read(off, 2)?;
        let block_len = u16::from_be_bytes([block_len_bytes[0], block_len_bytes[1]]);

        let block_data = self.file.read(off + 2, block_len as u64)?;
        let block = Block::decode(&block_data);

        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let metas = &self.block_meta;
        for idx in 0..metas.len() {
            let meta = &metas[idx];
            let (first_key, last_key) = (&meta.first_key, &meta.last_key);
            if key >= first_key.as_key_slice() {
                return idx;
            }
        }
        metas.len()         // invalid index
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
