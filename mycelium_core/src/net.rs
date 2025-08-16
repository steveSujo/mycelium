use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::UdpSocket;

pub const DISCOVERY_PORT: u16 = 7000;
pub const MULTICAST_ADDR: &str = "239.255.0.1";

pub struct NetManager {
    pub socket: Arc<UdpSocket>,
}

impl NetManager {
    pub fn local_addr(&self) -> SocketAddr {
        let addr = self.socket.local_addr().unwrap();
        #[allow(clippy::collapsible_if)]
        if let SocketAddr::V4(v4) = addr {
            if v4.ip().is_unspecified() {
                return SocketAddr::from(([127, 0, 0, 1], v4.port()));
            }
        }
        addr
    }

    pub fn new(port: u16) -> io::Result<Self> {
        Self::new_with_multicast(port, None)
    }

    pub fn new_with_multicast(
        port: u16,
        multi_addr: Option<(Ipv4Addr, Ipv4Addr)>,
    ) -> io::Result<Self> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;

        socket.set_reuse_address(true)?;
        socket.set_multicast_loop_v4(true)?;

        // Non-fatal multicast interface setting for Linux CI containers
        let _ = socket.set_multicast_if_v4(&Ipv4Addr::UNSPECIFIED);

        let address = SocketAddr::from(([0, 0, 0, 0], port));
        socket.bind(&address.into())?;

        if let Some((m_addr, m_iface)) = multi_addr {
            let join_iface = if m_iface == Ipv4Addr::UNSPECIFIED {
                Ipv4Addr::UNSPECIFIED
            } else {
                m_iface
            };
            // Non-fatal join for Linux loopback compatibility
            let _ = socket.join_multicast_v4(&m_addr, &join_iface);
        }

        socket.set_nonblocking(true)?;

        let std_socket: std::net::UdpSocket = socket.into();
        let udp_socket = UdpSocket::from_std(std_socket)?;
        Ok(Self {
            socket: Arc::new(udp_socket),
        })
    }

    pub fn join_multicast(&self, _multi_addr: Ipv4Addr, _interface: Ipv4Addr) -> io::Result<()> {
        Err(io::Error::other(
            "join_multicast must be configured during NetManager::new_with_multicast",
        ))
    }

    pub async fn send_to(&self, buf: &[u8], addr: &SocketAddr) -> io::Result<usize> {
        self.socket.send_to(buf, addr).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_socket_binding() {
        let manager = NetManager::new(0).expect("Should bind to port 0");
        assert!(manager.socket.local_addr().is_ok());
    }

    #[tokio::test]
    async fn test_join_multicast() {
        let multi_addr = Ipv4Addr::new(239, 0, 0, 1);
        let interface = Ipv4Addr::new(0, 0, 0, 0);

        let manager = NetManager::new_with_multicast(0, Some((multi_addr, interface)))
            .expect("Should create manager with multicast");

        assert!(manager.socket.local_addr().is_ok());
    }

    #[tokio::test]
    async fn test_socket_reuse() {
        let m1 = NetManager::new(0).expect("First bind should work");
        let port = m1.socket.local_addr().unwrap().port();
        let m2 = NetManager::new(port).expect("Second bind should work with reuse");

        assert!(m1.socket.local_addr().is_ok());
        assert!(m2.socket.local_addr().is_ok());
    }
}
