use crc32fast::Hasher;
use std::convert::TryFrom;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("Invalid magic bytes")]
    InvalidMagic,
    #[error("Unsupported protocol version: {0}")]
    UnsupportedVersion(u8),
    #[error("Invalid opcode: {0}")]
    InvalidOpcode(u8),
    #[error("Cluster token mismatch")]
    ClusterTokenMismatch,
    #[error("Payload length mismatch: expected {expected}, found {found}")]
    LengthMismatch { expected: usize, found: usize },
    #[error("Checksum validation failed")]
    ChecksumMismatch,
    #[error("Buffer too short")]
    BufferTooShort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    AnnounceMulticast = 0x01,
    PingUnicast = 0x02,
    PongUnicast = 0x03,
    GossipFull = 0x04,
    GossipDelta = 0x05,
    Refute = 0x06,
}

impl TryFrom<u8> for Opcode {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Opcode::AnnounceMulticast),
            0x02 => Ok(Opcode::PingUnicast),
            0x03 => Ok(Opcode::PongUnicast),
            0x04 => Ok(Opcode::GossipFull),
            0x05 => Ok(Opcode::GossipDelta),
            0x06 => Ok(Opcode::Refute),
            v => Err(ProtocolError::InvalidOpcode(v)),
        }
    }
}

pub const HEADER_SIZE: usize = 16;
pub const MAGIC: [u8; 2] = [0x4D, 0x59]; // 'MY'
pub const VERSION: u8 = 0x01;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub version: u8,
    pub opcode: Opcode,
    pub cluster_token: u32,
    pub length: u32,
    pub checksum: u32,
}

impl Header {
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..2].copy_from_slice(&MAGIC);
        buf[2] = self.version;
        buf[3] = self.opcode as u8;
        buf[4..8].copy_from_slice(&self.cluster_token.to_be_bytes());
        buf[8..12].copy_from_slice(&self.length.to_be_bytes());
        buf[12..16].copy_from_slice(&self.checksum.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8], expected_token: u32) -> Result<Self, ProtocolError> {
        if buf.len() < HEADER_SIZE {
            return Err(ProtocolError::BufferTooShort);
        }

        if buf[0..2] != MAGIC {
            return Err(ProtocolError::InvalidMagic);
        }

        let version = buf[2];
        if version != VERSION {
            return Err(ProtocolError::UnsupportedVersion(version));
        }

        let opcode = Opcode::try_from(buf[3])?;

        let cluster_token = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        if cluster_token != expected_token {
            return Err(ProtocolError::ClusterTokenMismatch);
        }

        let length = u32::from_be_bytes(buf[8..12].try_into().unwrap());
        let checksum = u32::from_be_bytes(buf[12..16].try_into().unwrap());

        Ok(Header {
            version,
            opcode,
            cluster_token,
            length,
            checksum,
        })
    }
}

pub struct Packet {
    pub header: Header,
    pub payload: Vec<u8>,
}

impl Packet {
    pub fn new(opcode: Opcode, cluster_token: u32, payload: Vec<u8>) -> Self {
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let checksum = hasher.finalize();

        let header = Header {
            version: VERSION,
            opcode,
            cluster_token,
            length: payload.len() as u32,
            checksum,
        };

        Packet { header, payload }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + self.payload.len());
        buf.extend_from_slice(&self.header.encode());
        buf.extend_from_slice(&self.payload);
        buf
    }

    pub fn decode(buf: &[u8], expected_token: u32) -> Result<Self, ProtocolError> {
        let header = Header::decode(buf, expected_token)?;

        let payload_start = HEADER_SIZE;
        let payload_end = payload_start + header.length as usize;

        if buf.len() < payload_end {
            return Err(ProtocolError::LengthMismatch {
                expected: header.length as usize,
                found: buf.len() - payload_start,
            });
        }

        let payload = buf[payload_start..payload_end].to_vec();

        let mut hasher = Hasher::new();
        hasher.update(&payload);
        if hasher.finalize() != header.checksum {
            return Err(ProtocolError::ChecksumMismatch);
        }

        Ok(Packet { header, payload })
    }
}
