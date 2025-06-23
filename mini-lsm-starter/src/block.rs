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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let buf_size = self.data.len() + self.offsets.len() * 2 + 2;
        let mut buf = BytesMut::with_capacity(buf_size);
        let total_records = self.offsets.len();

        // write entries
        buf.extend_from_slice(&self.data);
        for off in self.offsets.clone() {
            buf.put_u16(off);
        }
        buf.put_u16(total_records as u16);
        // assert!(buf.len() == 4096)
        buf.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let len = data.len();
        let total_records = u16::from_be_bytes([data[len - 2], data[len - 1]]) as usize;
        let off_start = len - 2 - total_records * 2;

        let entry_data = &data[0..off_start];
        let mut offsets = Vec::with_capacity(total_records);
        for idx in 0..total_records {
            let off = idx * 2;
            let off_value = u16::from_be_bytes([
                data[off_start + off], 
                data[off_start + off + 1]]);
            offsets.push(off_value);
        }

        Self {
            data: entry_data.to_vec(),
            offsets,
        }
    }
}



/*

学历：116641202105619205
学位：1166442021008265
 */