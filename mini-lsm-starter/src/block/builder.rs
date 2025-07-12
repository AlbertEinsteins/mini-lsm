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

use bytes::BufMut;

use crate::key::{Key, KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        let rest_space = block_size - 2; // the space of the number of elements used
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size: rest_space,
            first_key: Key::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // unimplemented!()
        let need_space = 2 + key.len() + 2 + value.len() + 2;
        if !self.is_empty() && self.block_size < need_space {
            return false;
        }

        let cur_pos = self.data.len() as u16;
        if cur_pos == 0 {
            self.first_key = key.to_key_vec();
        }
        self.offsets.push(cur_pos);
        // write <key_len, key, val_len, val>
        self.data.put_u16(key.len() as u16);
        self.data.append(&mut Vec::from(key.into_inner()));
        self.data.put_u16(value.len() as u16);
        self.data.append(&mut Vec::from(value));
        self.block_size = self.block_size.saturating_sub(need_space);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
