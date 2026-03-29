use std::sync::{Arc, Mutex};
use std::time::Instant;

use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Gauge, Paragraph};
use ratatui::Terminal;

use aegistorrent::network::coordinator::DownloadProgress;

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
        Some(s) if s < 60.0 => format!("{}s", s as u64),
        Some(s) if s < 3600.0 => format!("{}m {}s", s as u64 / 60, s as u64 % 60),
        Some(s) => format!("{}h {}m", s as u64 / 3600, (s as u64 % 3600) / 60),
    }
}

pub async fn run_dashboard(progress: Arc<Mutex<DownloadProgress>>) {
    use crossterm::execute;
    use crossterm::terminal::{
        disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
    };
    use std::io::stdout;
    use tokio::time::{interval, Duration};

    let (piece_count, bytes_total) = {
        let p = progress.lock().unwrap();
        (p.piece_count, p.bytes_total)
    };

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

    let started_at = Instant::now();
    let mut tick = interval(Duration::from_millis(125));

    loop {
        tick.tick().await;

        let snapshot = progress.lock().unwrap().clone();
        let elapsed = started_at.elapsed().as_secs_f64();
        let speed = if elapsed > 0.1 {
            snapshot.bytes_done as f64 / elapsed
        } else {
            0.0
        };
        let eta = if speed > 1.0 {
            let remaining = bytes_total.saturating_sub(snapshot.bytes_done);
            Some(remaining as f64 / speed)
        } else {
            None
        };
        let ratio = if piece_count > 0 {
            (snapshot.pieces_done as f64 / piece_count as f64).clamp(0.0, 1.0)
        } else {
            0.0
        };

        let view = DashboardView {
            progress: &snapshot,
            piece_count,
            bytes_total,
            ratio,
            speed,
            eta,
            elapsed,
        };
        let _ = terminal.draw(|frame| render(frame, &view));

        if snapshot.complete {
            break;
        }
    }

    let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen);
    let _ = disable_raw_mode();
}

struct DashboardView<'a> {
    progress: &'a DownloadProgress,
    piece_count: u32,
    bytes_total: u64,
    ratio: f64,
    speed: f64,
    eta: Option<f64>,
    elapsed: f64,
}

fn render_speed_bar(ratio: f64) -> String {
    let filled = (ratio * 10.0).round() as usize;
    let empty = 10 - filled.min(10);
    format!("{}{}", "█".repeat(filled.min(10)), "░".repeat(empty))
}

fn render(frame: &mut ratatui::Frame, view: &DashboardView) {
    let area = frame.area();

    let peer_rows = view.progress.peer_stats.len().max(1) as u16 + 2;
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(peer_rows),
            Constraint::Length(3),
        ])
        .split(area);

    let pct = view.ratio * 100.0;
    let title = Paragraph::new(Line::from(vec![
        Span::styled(
            "AegisTorrent",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(
            "  ·  {}/{}  ·  {:.1}%",
            format_bytes(view.progress.bytes_done),
            format_bytes(view.bytes_total),
            pct
        )),
    ]))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, chunks[0]);

    let gauge = Gauge::default()
        .block(Block::default().borders(Borders::ALL).title("Progress"))
        .gauge_style(Style::default().fg(Color::Green))
        .ratio(view.ratio)
        .label(format!(
            "{}/{} pieces",
            view.progress.pieces_done, view.piece_count
        ));
    frame.render_widget(gauge, chunks[1]);

    let stats_text = format!(
        "↓ {}   Peers: {}   ETA: {}   Elapsed: {:.0}s",
        format_speed(view.speed),
        view.progress.peers_connected,
        format_eta(view.eta),
        view.elapsed,
    );
    let stats_widget =
        Paragraph::new(stats_text).block(Block::default().borders(Borders::ALL).title("Stats"));
    frame.render_widget(stats_widget, chunks[2]);

    let peer_lines: Vec<Line> = view
        .progress
        .peer_stats
        .iter()
        .map(|p| {
            let id_short = if p.id.len() >= 4 { &p.id[..4] } else { &p.id };
            let (bar, speed_text) = if p.score <= 0.0 {
                (render_speed_bar(0.0), "BANNED".to_string())
            } else {
                let max_speed = view
                    .progress
                    .peer_stats
                    .iter()
                    .map(|s| s.speed_bps)
                    .fold(0.0_f64, f64::max);
                let ratio = if max_speed > 0.0 {
                    p.speed_bps / max_speed
                } else {
                    0.0
                };
                (render_speed_bar(ratio), format_speed(p.speed_bps))
            };
            let strike_str = if p.strikes > 0 {
                if p.score <= 0.0 {
                    format!(" ✗{}", p.strikes)
                } else {
                    format!(" ⚠{}", p.strikes)
                }
            } else {
                String::new()
            };
            Line::from(format!(
                "  {}  {}  {}  score:{:.2}  [{}]{}",
                id_short, bar, speed_text, p.score, p.pipeline, strike_str
            ))
        })
        .collect();
    let peers_widget =
        Paragraph::new(peer_lines).block(Block::default().borders(Borders::ALL).title("Peers"));
    frame.render_widget(peers_widget, chunks[3]);

    let heatmap: String = view
        .progress
        .piece_rarity
        .iter()
        .map(|&r| match r {
            0 => "[!!]",
            1 => "[░░]",
            2 => "[▓▓]",
            _ => "[██]",
        })
        .collect();
    let rarity_widget = Paragraph::new(heatmap)
        .block(Block::default().borders(Borders::ALL).title("Rarity"));
    frame.render_widget(rarity_widget, chunks[4]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_bytes_ranges() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(2048), "2.00 KB");
        assert_eq!(format_bytes(2 * 1_048_576), "2.00 MB");
        assert_eq!(format_bytes(3 * 1_073_741_824), "3.00 GB");
    }

    #[test]
    fn format_eta_ranges() {
        assert_eq!(format_eta(None), "∞");
        assert_eq!(format_eta(Some(30.0)), "30s");
        assert_eq!(format_eta(Some(90.0)), "1m 30s");
        assert_eq!(format_eta(Some(3660.0)), "1h 1m");
    }

    #[test]
    fn format_speed_display() {
        assert_eq!(format_speed(1024.0), "1.00 KB/s");
        assert_eq!(format_speed(5.0 * 1_048_576.0), "5.00 MB/s");
    }
}
