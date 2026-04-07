//! Multiplayer terminal leaderboard — powered by Meridian CRDTs.
//!
//! Each player gets a GCounter. Press Space to score a point.
//! Everyone connected to the same namespace sees the live leaderboard.
//!
//! # Usage
//!
//! ```bash
//! # Start Meridian server first
//! MERIDIAN_SIGNING_KEY=dev cargo run --bin meridian
//!
//! # Then run the game (pick a different player name per terminal)
//! cargo run -p ratatui-game -- --url ws://localhost:3000 --player alice --token <TOKEN>
//! cargo run -p ratatui-game -- --url ws://localhost:3000 --player bob   --token <TOKEN>
//! ```

use std::{
    io,
    sync::Arc,
    time::Duration,
};

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use meridian_client::{GCounterHandle, MeridianClient};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, List, ListItem, Paragraph},
    Terminal,
};
use tokio::sync::watch;
use tracing::info;

// ── CLI args ──────────────────────────────────────────────────────────────────

struct Args {
    url:       String,
    namespace: String,
    player:    String,
    token:     String,
    /// Other known player names to show on the leaderboard
    peers:     Vec<String>,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut url       = "ws://localhost:3000".to_owned();
    let mut namespace = "game-room".to_owned();
    let mut player    = "player1".to_owned();
    let mut token     = "dev".to_owned();
    let mut peers     = vec![];

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url"       => { if let Some(v) = args.next() { url = v; } }
            "--namespace" => { if let Some(v) = args.next() { namespace = v; } }
            "--player"    => { if let Some(v) = args.next() { player = v; } }
            "--token"     => { if let Some(v) = args.next() { token = v; } }
            "--peers"     => { if let Some(v) = args.next() { peers = v.split(',').map(str::to_owned).collect(); } }
            _ => {}
        }
    }

    Args { url, namespace, player, token, peers }
}

// ── App state ─────────────────────────────────────────────────────────────────

struct App {
    client:     Arc<MeridianClient>,
    my_handle:  GCounterHandle,
    /// (player_name, handle) pairs including self
    all_handles: Vec<(String, GCounterHandle)>,
    player:     String,
}

impl App {
    async fn new(args: &Args) -> Result<Self, Box<dyn std::error::Error>> {
        let client = MeridianClient::connect(&args.url, &args.namespace, &args.token).await?;

        let my_crdt_id = format!("gc:score:{}", args.player);
        let my_handle = client.gcounter(&my_crdt_id);

        // Build handle list: self first, then peers
        let mut all_handles = vec![(args.player.clone(), my_handle.clone())];
        for peer in &args.peers {
            let h = client.gcounter(&format!("gc:score:{peer}"));
            all_handles.push((peer.clone(), h));
        }

        Ok(Self {
            client,
            my_handle,
            all_handles,
            player: args.player.clone(),
        })
    }

    async fn score(&self) -> Result<(), meridian_client::ClientError> {
        self.my_handle.increment(1).await
    }

    fn leaderboard(&self) -> Vec<(String, u64, bool)> {
        let mut entries: Vec<(String, u64, bool)> = self.all_handles
            .iter()
            .map(|(name, h)| (name.clone(), h.value(), name == &self.player))
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries
    }

    fn my_score(&self) -> u64 {
        self.my_handle.value()
    }
}

// ── TUI rendering ─────────────────────────────────────────────────────────────

fn ui(frame: &mut ratatui::Frame, app: &App) {
    let area = frame.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // title
            Constraint::Min(0),     // leaderboard
            Constraint::Length(5),  // my score gauge
            Constraint::Length(3),  // help bar
        ])
        .split(area);

    // Title
    let title = Paragraph::new(Line::from(vec![
        Span::styled("🎮 Meridian Leaderboard ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::styled(format!("({})", app.player), Style::default().fg(Color::Yellow)),
    ]))
    .alignment(Alignment::Center)
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, chunks[0]);

    // Leaderboard
    let board = app.leaderboard();
    let max_score = board.first().map(|(_, s, _)| *s).unwrap_or(1).max(1);

    let items: Vec<ListItem> = board
        .iter()
        .enumerate()
        .map(|(i, (name, score, is_me))| {
            let medal = match i {
                0 => "🥇",
                1 => "🥈",
                2 => "🥉",
                _ => "  ",
            };
            let bar_width = (score * 20 / max_score) as usize;
            let bar = "█".repeat(bar_width) + &"░".repeat(20 - bar_width);

            let style = if *is_me {
                Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            };

            let label = format!("{medal} {name:<12} {bar} {score:>6} pts");
            ListItem::new(Line::from(Span::styled(label, style)))
        })
        .collect();

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(" Leaderboard "));
    frame.render_widget(list, chunks[1]);

    // My score gauge
    let my_score = app.my_score();
    let ratio = (my_score as f64 / max_score as f64).min(1.0);
    let gauge = Gauge::default()
        .block(Block::default().borders(Borders::ALL).title(format!(" Your score — {} ", app.player)))
        .gauge_style(Style::default().fg(Color::Green).bg(Color::DarkGray))
        .ratio(ratio)
        .label(format!("{my_score} pts"));
    frame.render_widget(gauge, chunks[2]);

    // Help bar
    let help = Paragraph::new(Line::from(vec![
        Span::styled(" [Space] ", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Span::raw("Score a point  "),
        Span::styled(" [q] ", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Span::raw("Quit"),
    ]))
    .alignment(Alignment::Center)
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(help, chunks[3]);
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("ratatui_game=info".parse()?),
        )
        .with_writer(io::stderr)
        .init();

    let args = parse_args();

    info!(player = %args.player, url = %args.url, "connecting to Meridian");
    let app = Arc::new(App::new(&args).await?);
    info!("connected — starting TUI");

    // Terminal setup
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Redraw trigger: watch channel updated on every key event and on a tick
    let (redraw_tx, mut redraw_rx) = watch::channel(());

    // Tick every 100 ms so remote score updates appear promptly
    let tick_tx = redraw_tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            let _ = tick_tx.send(());
        }
    });

    // Subscribe to local score changes → trigger redraw
    let score_tx = redraw_tx.clone();
    let _guard = app.my_handle.on_change(move |_| { let _ = score_tx.send(()); });

    // Event loop
    let result: Result<(), Box<dyn std::error::Error>> = 'main: loop {
        // Wait for redraw trigger
        let _ = redraw_rx.changed().await;
        terminal.draw(|f| ui(f, &app))?;

        // Drain all pending crossterm events (non-blocking)
        while event::poll(Duration::ZERO)? {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => {
                            break 'main Ok(());
                        }
                        KeyCode::Char(' ') => {
                            if let Err(e) = app.score().await {
                                tracing::warn!(error = %e, "failed to send score op");
                            }
                            let _ = redraw_tx.send(());
                        }
                        _ => {}
                    }
                }
                Event::Resize(_, _) => { let _ = redraw_tx.send(()); }
                _ => {}
            }
        }
    };

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    app.client.close().await;

    result
}
