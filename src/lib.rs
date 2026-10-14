// Copyright 2026 The Roc Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod column;
pub mod error;
pub mod exec;
pub mod expr;
pub mod kernel;
pub mod operator;
pub mod pipeline;

// Type-checked design draft for editor navigation; not part of the public API.

pub use asyncband::shutdown::Shutdown;

// Arrow is the interchange format only. Execution uses Roc's generated code.
pub(crate) mod arrow {
    pub mod array {
        pub use arrow_array::builder::*;
        pub use arrow_array::cast::AsArray;
        pub use arrow_array::*;
    }
    #[cfg(test)]
    pub use ::arrow::buffer;
    pub mod datatypes {
        pub use arrow_array::types::*;
        pub use arrow_schema::*;
    }
    pub mod record_batch {
        pub use arrow_array::{RecordBatch, RecordBatchOptions};
    }
    pub mod error {
        pub use arrow_schema::ArrowError;
    }
    #[cfg(test)]
    pub use ::arrow::compute;
}
