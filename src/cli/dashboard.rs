use std::sync::{Arc, Mutex};
use std::time::Instant;

use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Gauge, Paragraph};
use ratatui::Terminal;

#[derive(Debug, Clone)]
pub struct DownloadStats {
    pub piece_count: u32,
    pub pieces_done: u32,
    pub peers_connected: usize,
    pub bytes_total: u64,
    pub bytes_done: u64,
    pub started_at: Instant,
    pub complete: bool,
}

impl DownloadStats {
    pub fn new(piece_count: u32, bytes_total: u64) -> Self {
        Self {
            piece_count,
            pieces_done: 0,
            peers_connected: 0,
            bytes_total,
            bytes_done: 0,
            started_at: Instant::now(),
            complete: false,
        }
    }

    pub fn progress_ratio(&self) -> f64 {
        if self.piece_count == 0 {
            return 0.0;
        }
        self.pieces_done as f64 / self.piece_count as f64
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    pub fn speed_bps(&self) -> f64 {
        let elapsed = self.elapsed_secs();
        if elapsed < 0.1 {
            return 0.0;
        }
        self.bytes_done as f64 / elapsed
    }

    pub fn eta_secs(&self) -> Option<f64> {
        let speed = self.speed_bps();
        if speed < 1.0 {
            return None;
        }
        let remaining = self.bytes_total.saturating_sub(self.bytes_done);
        Some(remaining as f64 / speed)
    }
}

pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.2} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

pub fn format_speed(bps: f64) -> String {
    format!("{}/s", format_bytes(bps as u64))
}

pub fn format_eta(secs: Option<f64>) -> String {
    match secs {
        None => "∞".to_string(),
        Some(s) if s < 60.0 => format!("{:.0}s", s),
        Some(s) if s < 3600.0 => format!("{:.0}m {:.0}s", s / 60.0, s % 60.0),
        Some(s) => format!("{:.0}h {:.0}m", s / 3600.0, (s % 3600.0) / 60.0),
    }
}

pub async fn run_dashboard(stats: Arc<Mutex<DownloadStats>>) {
    use crossterm::{
        execute,
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    };
    use std::io::stdout;
    use tokio::time::{interval, Duration};

    let _ = enable_raw_mode();
    let mut out = stdout();
    let _ = execute!(out, EnterAlternateScreen);

    let backend = CrosstermBackend::new(stdout());
    let mut terminal = match Terminal::new(backend) {
        Ok(t) => t,
        Err(_) => {
            let _ = disable_raw_mode();
            return;
        }
    };

    let mut tick = interval(Duration::from_millis(125));

    loop {
        tick.tick().await;

        let snapshot = {
            let s = stats.lock().unwrap();
            s.clone()
        };

        let complete = snapshot.complete;

        let _ = terminal.draw(|frame| render(frame, &snapshot));

        if complete {
            break;
        }
    }

    let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen);
    let _ = disable_raw_mode();
}

fn render(frame: &mut ratatui::Frame, stats: &DownloadStats) {
    let area = frame.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(0),
        ])
        .split(area);

    let pct = stats.progress_ratio() * 100.0;
    let title = Paragraph::new(Line::from(vec![
        Span::styled(
            "AegisTorrent",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(
            "  ·  {}/{}  ·  {:.1}%",
            format_bytes(stats.bytes_done),
            format_bytes(stats.bytes_total),
            pct
        )),
    ]))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, chunks[0]);

    let gauge = Gauge::default()
        .block(Block::default().borders(Borders::ALL).title("Progress"))
        .gauge_style(Style::default().fg(Color::Green))
        .ratio(stats.progress_ratio().clamp(0.0, 1.0))
        .label(format!(
            "{}/{} pieces",
            stats.pieces_done, stats.piece_count
        ));
    frame.render_widget(gauge, chunks[1]);

    let stats_text = format!(
        "↓ {}   Peers: {}   ETA: {}   Elapsed: {:.0}s",
        format_speed(stats.speed_bps()),
        stats.peers_connected,
        format_eta(stats.eta_secs()),
        stats.elapsed_secs(),
    );
    let stats_widget = Paragraph::new(stats_text)
        .block(Block::default().borders(Borders::ALL).title("Stats"));
    frame.render_widget(stats_widget, chunks[2]);
}
