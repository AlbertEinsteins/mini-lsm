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

use std::{fs::Metadata, path::Path};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, SsTable};
use crate::{block::BlockBuilder, key::{Key, KeyBytes, KeySlice}, lsm_storage::BlockCache, table::FileObject};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            let block_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let encoded_bytes = block_builder.build().encode();

            let off = self.data.len();
            self.meta.push(BlockMeta {
                offset: off,
                first_key: KeyBytes::from_bytes(Bytes::from(self.first_key.clone())),
                last_key: KeyBytes::from_bytes(Bytes::from(self.last_key.clone()))
            });
            self.data.extend_from_slice(&encoded_bytes);
            let _ = self.builder.add(key, value);
            self.first_key = key.into_inner().to_vec();
        }

        // set last_key
        self.last_key = key.into_inner().to_vec();

        if self.first_key.is_empty() {
            self.first_key = key.into_inner().to_vec();
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let mut this = self;
        // flush block
        if !this.builder.is_empty() {
            let encoded_bytes = this.builder.build().encode();
            let encoded_len = encoded_bytes.len() as u16;
            let off = this.data.len();
            this.meta.push(BlockMeta {
                offset: off,
                first_key: Key::from_bytes(Bytes::from(this.first_key)),
                last_key: Key::from_bytes(Bytes::from(this.last_key))
            });
            this.data.extend_from_slice(&encoded_len.to_be_bytes());
            this.data.extend_from_slice(&encoded_bytes);
        }

        // encode metas
        let mut buf = Vec::new();
        let meta_offset = this.data.len() as u16;

        BlockMeta::encode_block_meta(&this.meta, &mut buf);
        this.data.extend(buf);
        this.data.extend_from_slice(&meta_offset.to_be_bytes());

        // create file and wirte data
        let file = FileObject::create(path.as_ref(), this.data)?;
        
        Ok(SsTable {
            file,
            block_meta: this.meta.clone(),
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            first_key: this.meta.first().unwrap().first_key.clone(),
            last_key: this.meta.last().unwrap().last_key.clone(),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
