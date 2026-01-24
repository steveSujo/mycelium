#![allow(unused_mut)]

use mycelium_core::crdt::PeerStatus;
use mycelium_core::{
    AnnouncePayload, DISCOVERY_PORT, GossipManager, MULTICAST_ADDR, NetManager, NodeState,
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    crossterm::{
        event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
        execute,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    },
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style, Stylize},
    symbols::border,
    text::{Line, Text},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState},
};
use std::{
    env, io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, mpsc, watch},
    time::interval,
};

#[derive(Debug, Clone)]
struct PeerEntry {
    node_id: String,
    address: SocketAddr,
    incarnation: u64,
    status: PeerStatus,
    last_seen: Instant,
}

#[derive(Debug, Clone)]
enum AppEvent {
    PeerUpdate(Vec<PeerEntry>),
    LocalNodeInfo {
        node_id: String,
        incarnation: u64,
        cluster_token: u32,
        local_addr: SocketAddr,
    },
    Log(String),
}

struct App {
    peer_entries: Vec<PeerEntry>,
    local_node_id: String,
    local_incarnation: u64,
    cluster_token: u32,
    local_addr: SocketAddr,
    table_state: TableState,
    logs: Vec<String>,
    max_logs: usize,
    last_tick: Instant,
    running: bool,
}

impl App {
    fn new() -> Self {
        Self {
            peer_entries: Vec::new(),
            local_node_id: String::new(),
            local_incarnation: 0,
            cluster_token: 0,
            local_addr: "0.0.0.0:0".parse().unwrap(),
            table_state: TableState::default(),
            logs: Vec::new(),
            max_logs: 100,
            last_tick: Instant::now(),
            running: true,
        }
    }

    fn add_log(&mut self, msg: String) {
        let timestamp = chrono::Local::now().format("%H:%M:%S%.3f").to_string();
        self.logs.push(format!("[{}] {}", timestamp, msg));
        if self.logs.len() > self.max_logs {
            self.logs.drain(0..self.logs.len() - self.max_logs);
        }
    }

    fn next_row(&mut self) {
        let i = match self.table_state.selected() {
            Some(i) => {
                if i >= self.peer_entries.len().saturating_sub(1) {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.table_state.select(Some(i));
    }

    fn previous_row(&mut self) {
        let i = match self.table_state.selected() {
            Some(i) => {
                if i == 0 {
                    self.peer_entries.len().saturating_sub(1)
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.table_state.select(Some(i));
    }

    fn update_peers(&mut self, entries: Vec<PeerEntry>) {
        self.peer_entries = entries;
        if self.table_state.selected().is_some() {
            let selected = self.table_state.selected().unwrap();
            if selected >= self.peer_entries.len() {
                self.table_state
                    .select(Some(self.peer_entries.len().saturating_sub(1)));
            }
        }
    }

    fn set_local_info(
        &mut self,
        node_id: String,
        incarnation: u64,
        cluster_token: u32,
        local_addr: SocketAddr,
    ) {
        self.local_node_id = node_id;
        self.local_incarnation = incarnation;
        self.cluster_token = cluster_token;
        self.local_addr = local_addr;
    }
}

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    }
}

fn format_node_id(id: &str) -> String {
    if id.len() > 12 {
        format!("{}…{}", &id[..8], &id[id.len() - 4..])
    } else {
        id.to_string()
    }
}

fn status_style(status: PeerStatus) -> Style {
    match status {
        PeerStatus::Alive => Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD),
        PeerStatus::Suspect => Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
        PeerStatus::Dead => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
    }
}

fn render_ui(frame: &mut Frame, app: &mut App) {
    let size = frame.area();

    // Define layout
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5), // Header
            Constraint::Min(10),   // Peer table
            Constraint::Length(8), // Logs
            Constraint::Length(2), // Footer
        ])
        .split(size);

    // Header
    let header_block = Block::default()
        .borders(Borders::ALL)
        .border_set(border::ROUNDED)
        .title(" Mycelium Node ")
        .title_alignment(Alignment::Center)
        .style(Style::default().fg(Color::Cyan));

    let header_text = Text::from(vec![
        Line::from(vec![
            "Node ID: ".into(),
            format_node_id(&app.local_node_id).bold().cyan(),
            "  |  Incarnation: ".into(),
            app.local_incarnation.to_string().bold().yellow(),
            "  |  Token: ".into(),
            app.cluster_token.to_string().bold().magenta(),
        ]),
        Line::from(vec![
            "Listening on: ".into(),
            app.local_addr.to_string().bold().green(),
        ]),
        Line::from(vec![
            "Peers: ".into(),
            app.peer_entries.len().to_string().bold().white(),
            "  |  Alive: ".into(),
            app.peer_entries
                .iter()
                .filter(|p| p.status == PeerStatus::Alive)
                .count()
                .to_string()
                .bold()
                .green(),
            "  |  Suspect: ".into(),
            app.peer_entries
                .iter()
                .filter(|p| p.status == PeerStatus::Suspect)
                .count()
                .to_string()
                .bold()
                .yellow(),
            "  |  Dead: ".into(),
            app.peer_entries
                .iter()
                .filter(|p| p.status == PeerStatus::Dead)
                .count()
                .to_string()
                .bold()
                .red(),
        ]),
    ]);

    let header_para = Paragraph::new(header_text).block(header_block);
    frame.render_widget(header_para, chunks[0]);

    // Peer Table
    let rows: Vec<Row> = app
        .peer_entries
        .iter()
        .map(|peer| {
            let age = format_duration(app.last_tick.saturating_duration_since(peer.last_seen));
            let cells = vec![
                Cell::from(format_node_id(&peer.node_id)).style(Style::default().fg(Color::Cyan)),
                Cell::from(peer.address.to_string()).style(Style::default().fg(Color::White)),
                Cell::from(peer.incarnation.to_string()).style(Style::default().fg(Color::Yellow)),
                Cell::from(format!("{:?}", peer.status)).style(status_style(peer.status)),
                Cell::from(age).style(Style::default().fg(Color::Gray)),
            ];
            Row::new(cells).height(1)
        })
        .collect();

    let widths = [
        Constraint::Length(20), // Node ID
        Constraint::Length(22), // Address
        Constraint::Length(12), // Incarnation
        Constraint::Length(10), // Status
        Constraint::Min(10),    // Last Seen
    ];

    let table = Table::new(rows, widths)
        .header(
            Row::new(vec![
                Cell::from("Node ID").style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Cell::from("Address").style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Cell::from("Incarnation").style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Cell::from("Status").style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Cell::from("Last Seen").style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
            ])
            .bottom_margin(1),
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_set(border::ROUNDED)
                .title(" Peer Table (↑/↓ to navigate) ")
                .title_alignment(Alignment::Center)
                .style(Style::default().fg(Color::Cyan)),
        )
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .column_spacing(2);

    frame.render_stateful_widget(table, chunks[1], &mut app.table_state);

    // Logs
    let log_lines: Vec<Line> = app
        .logs
        .iter()
        .rev()
        .take(5)
        .map(|log| Line::from(log.as_str()))
        .collect();

    let log_block = Block::default()
        .borders(Borders::ALL)
        .border_set(border::ROUNDED)
        .title(" Recent Logs ")
        .title_alignment(Alignment::Center)
        .style(Style::default().fg(Color::Cyan));

    let log_para = Paragraph::new(log_lines)
        .block(log_block)
        .alignment(Alignment::Left);
    frame.render_widget(log_para, chunks[2]);

    // Footer
    let footer_text = Line::from(vec![
        " ".into(),
        "q".bold().cyan(),
        " Quit  ".into(),
        "r".bold().cyan(),
        " Refresh  ".into(),
        "↑/↓".bold().cyan(),
        " Navigate  ".into(),
        "Press ".into(),
        "Ctrl+C".bold().red(),
        " to force quit".into(),
    ]);

    let footer_para = Paragraph::new(footer_text)
        .style(Style::default().fg(Color::Gray))
        .alignment(Alignment::Center);
    frame.render_widget(footer_para, chunks[3]);
}

async fn run_gossip_loop(
    gossip_manager: Arc<Mutex<GossipManager>>,
    local_info: (String, u64, u32, SocketAddr),
    event_tx: mpsc::UnboundedSender<AppEvent>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let (node_id, incarnation, cluster_token, local_addr) = local_info;

    // Send initial local node info
    let _ = event_tx.send(AppEvent::LocalNodeInfo {
        node_id: node_id.clone(),
        incarnation,
        cluster_token,
        local_addr,
    });

    // Send initial announce
    {
        let mut gm = gossip_manager.lock().await;
        let announce = AnnouncePayload {
            node_id: node_id.clone(),
            incarnation,
            address: local_addr,
        };
        let payload = serde_json::to_vec(&announce).unwrap();
        let multi_addr: SocketAddr = format!("{}:{}", MULTICAST_ADDR, DISCOVERY_PORT)
            .parse()
            .unwrap();
        let _ = gm.send_announce(multi_addr, payload).await;
        let _ = event_tx.send(AppEvent::Log("Sent initial ANNOUNCE_MULTICAST".to_string()));
    }

    let mut tick_interval = interval(Duration::from_secs(1));
    let mut announce_interval = interval(Duration::from_secs(2));
    let mut peer_sync_interval = interval(Duration::from_millis(500));

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
            _ = tick_interval.tick() => {
                let mut gm = gossip_manager.lock().await;
                gm.tick();
            }
            _ = announce_interval.tick() => {
                let mut gm = gossip_manager.lock().await;
                let announce = AnnouncePayload {
                    node_id: node_id.clone(),
                    incarnation,
                    address: local_addr,
                };
                let payload = serde_json::to_vec(&announce).unwrap();
                let multi_addr: SocketAddr = format!("{}:{}", MULTICAST_ADDR, DISCOVERY_PORT).parse().unwrap();
                if let Err(e) = gm.send_announce(multi_addr, payload).await {
                    let _ = event_tx.send(AppEvent::Log(format!("Failed to send announce: {}", e)));
                }
            }
            _ = peer_sync_interval.tick() => {
                let gm = gossip_manager.lock().await;
                let mut entries = Vec::new();
                for (nid, state) in gm.peer_table.iter() {
                    // Skip self
                    if nid.as_str() == node_id.as_str() {
                        continue;
                    }
                    entries.push(PeerEntry {
                        node_id: nid.clone(),
                        address: state.address,
                        incarnation: state.incarnation,
                        status: state.status,
                        last_seen: gm.peer_table.last_seen(nid).unwrap_or_else(Instant::now),
                    });
                }
                let _ = event_tx.send(AppEvent::PeerUpdate(entries));
            }
        }
    }
}

async fn run_receiver_loop(
    socket: Arc<tokio::net::UdpSocket>,
    gossip_manager: Arc<Mutex<GossipManager>>,
    event_tx: mpsc::UnboundedSender<AppEvent>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut buf = [0u8; 4096];
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
            result = socket.recv_from(&mut buf) => {
                match result {
                    Ok((n, addr)) => {
                        let payload = &buf[..n];
                        let mut gm = gossip_manager.lock().await;
                        if let Err(e) = gm.handle_packet(payload) {
                            let _ = event_tx.send(AppEvent::Log(format!("Packet error from {}: {}", addr, e)));
                        } else {
                            let _ = event_tx.send(AppEvent::Log(format!("Received packet from {} ({} bytes)", addr, n)));
                        }
                    }
                    Err(e) => {
                        let _ = event_tx.send(AppEvent::Log(format!("Socket error: {}", e)));
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    let args: Vec<String> = env::args().collect();

    let mut storage_path = "./tmp".to_string();
    let mut token_str = "12345".to_string();

    for i in 0..args.len() {
        if args[i] == "--storage" && i + 1 < args.len() {
            storage_path = args[i + 1].clone();
        }
        if args[i] == "--token" && i + 1 < args.len() {
            token_str = args[i + 1].clone();
        }
    }

    let token: u32 = if let Ok(val) = token_str.parse::<u32>() {
        val
    } else {
        token_str
            .as_bytes()
            .iter()
            .fold(0u32, |acc, &b| acc.wrapping_add(b as u32))
    };

    // Initialize node state
    let mut node_state = NodeState::init(&storage_path);
    node_state.cluster_token = token;
    let node_id = node_state.node_id().to_string();
    let incarnation = node_state.incarnation();

    // Setup network
    let net_manager = NetManager::new_with_multicast(
        DISCOVERY_PORT,
        Some((
            MULTICAST_ADDR.parse().unwrap(),
            std::net::Ipv4Addr::UNSPECIFIED,
        )),
    )?;
    let local_addr = net_manager.local_addr();
    let socket = net_manager.socket.clone();

    let gossip_manager = Arc::new(Mutex::new(GossipManager::new(net_manager, node_state)));

    // Setup TUI
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Channels for communication
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<AppEvent>();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut app = App::new();
    app.set_local_info(node_id.clone(), incarnation, token, local_addr);
    app.add_log(format!("Starting Mycelium Node (token: {})", token));
    app.add_log(format!("Storage: {}", storage_path));
    app.add_log(format!("Node ID: {}", node_id));
    app.add_log(format!("Listening on: {}", local_addr));

    // Spawn gossip loop
    let gossip_manager_clone = Arc::clone(&gossip_manager);
    let event_tx_gossip = event_tx.clone();
    let shutdown_rx_gossip = shutdown_rx.clone();
    let local_info = (node_id.clone(), incarnation, token, local_addr);
    tokio::spawn(async move {
        run_gossip_loop(
            gossip_manager_clone,
            local_info,
            event_tx_gossip,
            shutdown_rx_gossip,
        )
        .await;
    });

    // Spawn receiver loop
    let gossip_manager_clone = Arc::clone(&gossip_manager);
    let event_tx_recv = event_tx.clone();
    let shutdown_rx_recv = shutdown_rx.clone();
    tokio::spawn(async move {
        run_receiver_loop(
            socket,
            gossip_manager_clone,
            event_tx_recv,
            shutdown_rx_recv,
        )
        .await;
    });

    // Main TUI loop
    let mut last_render = Instant::now();
    while app.running {
        // Handle events
        while let Ok(event) = event_rx.try_recv() {
            match event {
                AppEvent::PeerUpdate(entries) => {
                    app.update_peers(entries);
                    app.last_tick = Instant::now();
                }
                AppEvent::LocalNodeInfo {
                    node_id,
                    incarnation,
                    cluster_token,
                    local_addr,
                } => {
                    app.set_local_info(node_id, incarnation, cluster_token, local_addr);
                }
                AppEvent::Log(msg) => {
                    app.add_log(msg);
                }
            }
        }
        // Handle keyboard input
        #[allow(clippy::collapsible_if)]
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            app.running = false;
                        }
                        KeyCode::Char('r') => {
                            app.add_log("Manual refresh triggered".to_string());
                        }
                        KeyCode::Up => {
                            app.previous_row();
                        }
                        KeyCode::Down => {
                            app.next_row();
                        }
                        _ => {}
                    }
                }
            }
        }

        // Render at ~30fps
        if last_render.elapsed() >= Duration::from_millis(33) {
            terminal.draw(|f| render_ui(f, &mut app))?;
            last_render = Instant::now();
        }
    }

    // Cleanup
    let _ = shutdown_tx.send(true);
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}
