#![allow(dead_code)]

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeRequest {
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeStreamInfo {
    pub sample_rate: Option<u32>,
    pub channel_count: Option<u32>,
    pub bit_depth: Option<u32>,
}
