mod kafka;

use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use std::fs::File;
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Span, Spans},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame, Terminal,
};

use kafka::KafkaClient;

struct App {
    kafka_client: KafkaClient,
    topics: Vec<String>,
    filtered_topics: Vec<String>,
    selected_topic_index: Option<usize>,
    topic_config: Option<Vec<(String, String)>>,
    selected_config_index: Option<usize>,
    state: AppState,
    filter_input: String,
    filter_mode: bool,
    // Live tail related fields
    tail_messages: Arc<Mutex<Vec<String>>>,
    tail_scroll: u16,
    tail_running: bool,
    tail_topic: Option<String>,
}

enum AppState {
    Topics,
    TopicDetail,
}

impl App {
    fn new(kafka_client: KafkaClient) -> Result<Self> {
        let topics = kafka_client.list_topics()?;
        let app = App {
            kafka_client,
            topics: topics.clone(),
            filtered_topics: topics,
            selected_topic_index: Some(0),
            topic_config: None,
            selected_config_index: None,
            state: AppState::Topics,
            filter_input: String::new(),
            filter_mode: false,

            // Initialize live tail fields
            tail_messages: Arc::new(Mutex::new(Vec::new())),
            tail_scroll: 0,
            tail_running: false,
            tail_topic: None,
        };

        Ok(app)
    }

    async fn update_topic_config(&mut self) {
        if let Some(idx) = self.selected_topic_index {
            if let Some(topic) = self.filtered_topics.get(idx) {
                if let Ok(config) = self.kafka_client.get_topic_config(topic).await {
                    let config_vec: Vec<(String, String)> = config.into_iter().collect();
                    self.topic_config = Some(config_vec);
                    self.selected_config_index = Some(0);
                }
            }
        }
    }

    async fn start_tail(&mut self) {
        let topic_to_tail = if let Some(idx) = self.selected_topic_index {
            if let Some(topic) = self.filtered_topics.get(idx) {
                // Only start a new tail if we're not already tailing this topic
                if self.tail_topic.as_ref() != Some(topic) {
                    Some(topic.to_string())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(topic) = topic_to_tail {
            // Stop any existing tail
            self.stop_tail().await;

            // Clear existing messages
            let mut messages = self.tail_messages.lock().await;
            messages.clear();

            // Create a channel for receiving messages
            let (tx, mut rx) = mpsc::channel(100);

            // Start consuming messages
            if self
                .kafka_client
                .consume_topic_messages(&topic, tx)
                .await
                .is_ok()
            {
                self.tail_running = true;
                self.tail_topic = Some(topic.clone());

                // Start a task to receive messages
                let tail_messages = Arc::clone(&self.tail_messages);
                let topic_clone = topic.clone();

                tokio::spawn(async move {
                    while let Some(msg) = rx.recv().await {
                        let mut messages = tail_messages.lock().await;
                        // Add topic prefix to message to ensure we know which topic it came from
                        messages.push(format!("[{}] {}", topic_clone, msg));
                    }
                });
            }
        }
    }

    async fn stop_tail(&mut self) {
        if let Some(topic) = &self.tail_topic {
            // Stop the consumer for this topic
            self.kafka_client.stop_consumer(topic).await;
        }
        self.tail_running = false;
        self.tail_topic = None;
    }

    async fn on_key(&mut self, key: KeyCode) {
        match key {
            KeyCode::Char('Q') => {
                // Stop all consumers before quitting
                self.kafka_client.stop_all_consumers().await;
            }
            KeyCode::Char('b') => {
                if let AppState::TopicDetail = self.state {
                    self.state = AppState::Topics;
                }
            }
            KeyCode::Char('T') => {
                self.state = AppState::Topics;
                self.filter_mode = false;
                self.filter_input.clear();
                self.apply_filter();
            }
            KeyCode::Char('/') => {
                if self.filter_mode {
                    self.filter_mode = false;
                    self.filter_input.clear();
                    self.apply_filter();
                } else {
                    self.filter_mode = true;
                }
            }
            KeyCode::Esc => {
                self.filter_mode = false;
                self.filter_input.clear();
                self.apply_filter();
            }
            KeyCode::Backspace => {
                if self.filter_mode {
                    self.filter_input.pop();
                    self.apply_filter();
                }
            }
            KeyCode::Char(c) => {
                if self.filter_mode {
                    self.filter_input.push(c);
                    self.apply_filter();
                }
            }
            KeyCode::Up => match self.state {
                AppState::Topics => {
                    if let Some(idx) = self.selected_topic_index {
                        if idx > 0 {
                            self.selected_topic_index = Some(idx - 1);
                        } else {
                            self.selected_topic_index = Some(self.filtered_topics.len() - 1);
                        }
                        self.update_topic_config().await;
                        // Start tailing the newly selected topic
                        self.start_tail().await;
                    }
                }
                AppState::TopicDetail => {
                    if let Some(idx) = self.selected_config_index {
                        if let Some(config) = &self.topic_config {
                            if idx > 0 {
                                self.selected_config_index = Some(idx - 1);
                            } else {
                                self.selected_config_index = Some(config.len() - 1);
                            }
                        }
                    }
                }
            },
            KeyCode::Down => match self.state {
                AppState::Topics => {
                    if let Some(idx) = self.selected_topic_index {
                        if idx < self.filtered_topics.len() - 1 {
                            self.selected_topic_index = Some(idx + 1);
                        } else {
                            self.selected_topic_index = Some(0);
                        }
                        self.update_topic_config().await;
                        // Start tailing the newly selected topic
                        self.start_tail().await;
                    }
                }
                AppState::TopicDetail => {
                    if let Some(idx) = self.selected_config_index {
                        if let Some(config) = &self.topic_config {
                            if idx < config.len() - 1 {
                                self.selected_config_index = Some(idx + 1);
                            } else {
                                self.selected_config_index = Some(0);
                            }
                        }
                    }
                }
            },
            KeyCode::Enter => {
                if let AppState::Topics = self.state {
                    if let Some(idx) = self.selected_topic_index {
                        if let Some(topic) = self.filtered_topics.get(idx) {
                            if let Ok(config) = self.kafka_client.get_topic_config(topic).await {
                                let config_vec: Vec<(String, String)> =
                                    config.into_iter().collect();
                                self.topic_config = Some(config_vec);
                                self.selected_config_index = Some(0);
                                self.state = AppState::TopicDetail;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn apply_filter(&mut self) {
        if let AppState::Topics = self.state {
            if self.filter_input.is_empty() {
                self.filtered_topics = self.topics.clone();
            } else {
                self.filtered_topics = self
                    .topics
                    .iter()
                    .filter(|t| t.to_lowercase().contains(&self.filter_input.to_lowercase()))
                    .cloned()
                    .collect();
            }
            // Reset selection if it's out of bounds
            if let Some(idx) = self.selected_topic_index {
                if idx >= self.filtered_topics.len() {
                    self.selected_topic_index = if self.filtered_topics.is_empty() {
                        None
                    } else {
                        Some(0)
                    };
                }
            } else if !self.filtered_topics.is_empty() {
                self.selected_topic_index = Some(0);
            }
        }
    }
}

// Add a function to restore terminal state
fn restore_terminal() {
    let _ = disable_raw_mode();
    let mut stdout = io::stdout();
    let _ = execute!(stdout, LeaveAlternateScreen, DisableMouseCapture);
    let _ = crossterm::terminal::disable_raw_mode();
    let _ = crossterm::cursor::Show;
}

fn main() -> Result<()> {
    // Set up panic hook to restore terminal state
    let panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // Restore terminal state
        restore_terminal();
        // Call the original panic hook
        panic_hook(panic_info);
    }));

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create Kafka client with error handling
    let kafka_client = match KafkaClient::new("localhost:9092") {
        Ok(client) => client,
        Err(e) => {
            // Log error to file
            let mut file = File::create("ktui_errors.log")?;
            writeln!(file, "Failed to connect to Kafka: {}", e)?;

            // Display error and wait for user input
            terminal.draw(|f| {
                let size = f.size();
                let block = Block::default()
                    .title("Kafka Connection Error")
                    .borders(Borders::ALL);
                let error_text = format!(
                    "Failed to connect to Kafka: {}\n\nError logged to ktui_errors.log\n\nPress any key to exit...",
                    e
                );
                let paragraph = Paragraph::new(error_text)
                    .block(block)
                    .wrap(tui::widgets::Wrap { trim: true });
                f.render_widget(paragraph, size);
            })?;

            // Wait for a key press
            if let Event::Key(_) = event::read()? {
                // Restore terminal and exit
                restore_terminal();
                return Ok(());
            }
            return Ok(());
        }
    };

    // Create app state
    let mut app = App::new(kafka_client)?;

    // Main loop
    let runtime = tokio::runtime::Runtime::new()?;
    let mut last_refresh = std::time::Instant::now();
    let refresh_interval = Duration::from_millis(100); // Refresh every 100ms

    loop {
        // Check if it's time to refresh the UI
        let now = std::time::Instant::now();
        let should_refresh = now.duration_since(last_refresh) >= refresh_interval;

        // Draw the UI
        terminal.draw(|f| ui(f, &app))?;

        runtime.block_on(app.start_tail());
        // Handle input with a timeout
        if event::poll(Duration::from_millis(10))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('Q') && !app.filter_mode {
                    // Stop all consumers before quitting
                    runtime.block_on(app.kafka_client.stop_all_consumers());
                    break;
                }
                // Call on_key in the async runtime
                runtime.block_on(app.on_key(key.code));
            }
        }

        // Update the last refresh time
        if should_refresh {
            last_refresh = now;
        }
    }

    // Restore terminal
    restore_terminal();

    Ok(())
}

fn ui<B: Backend>(f: &mut Frame<B>, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(0),
        ])
        .split(f.size());

    let title = match app.state {
        AppState::Topics => "Topics (T) | Filter (/) | Quit (Q)",
        AppState::TopicDetail => "Topic Detail (b to go back) | Quit (Q)",
    };

    let title = Paragraph::new(title)
        .style(Style::default().fg(Color::Cyan))
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(title, chunks[0]);

    // Filter input
    let filter_text = if app.filter_mode {
        format!("Filter: {}", app.filter_input)
    } else {
        "".to_string()
    };
    let filter = Paragraph::new(filter_text).block(Block::default().borders(Borders::ALL));
    f.render_widget(filter, chunks[1]);

    match app.state {
        AppState::Topics => {
            let content_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
                .split(chunks[2]);

            // Topics list on the left
            let items: Vec<ListItem> = app
                .filtered_topics
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    let style = if Some(i) == app.selected_topic_index {
                        Style::default()
                            .fg(Color::Black)
                            .bg(Color::White)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    };
                    ListItem::new(vec![Spans::from(Span::raw(t))]).style(style)
                })
                .collect();

            let topics = List::new(items)
                .block(Block::default().title("Topics").borders(Borders::ALL))
                .highlight_style(Style::default().add_modifier(Modifier::BOLD))
                .highlight_symbol("> ");

            f.render_widget(topics, content_chunks[0]);

            // Right panel split into two stacked panels
            let right_panels = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(content_chunks[1]);

            // Topic config on the top right
            let config_text = if let Some(config) = &app.topic_config {
                config
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, v))
                    .collect::<Vec<String>>()
                    .join("\n")
            } else {
                "Select a topic to view its configuration".to_string()
            };

            let config = Paragraph::new(config_text)
                .block(
                    Block::default()
                        .title("Topic Configuration")
                        .borders(Borders::ALL),
                )
                .wrap(tui::widgets::Wrap { trim: true });

            f.render_widget(config, right_panels[0]);

            // Live tail of topic data on the bottom right
            let tail_text = if let Some(idx) = app.selected_topic_index {
                if let Some(topic) = app.filtered_topics.get(idx) {
                    if app.tail_running {
                        let messages = app.tail_messages.blocking_lock();
                        if messages.is_empty() {
                            format!("Waiting for messages from topic: {}", topic)
                        } else {
                            // Only show actual messages, not error messages
                            messages
                                .iter()
                                .rev() // Reverse to get latest messages first
                                .filter(|msg| !msg.starts_with("Error:")) // Filter out error messages
                                .take(10) // Take only the last 10 messages
                                .collect::<Vec<_>>()
                                .into_iter()
                                .rev() // Reverse back to show in chronological order
                                .cloned()
                                .collect::<Vec<_>>()
                                .join("\n")
                        }
                    } else {
                        format!("Live tail not running for topic: {}", topic)
                    }
                } else {
                    "No topic selected".to_string()
                }
            } else {
                "No topic selected".to_string()
            };

            let tail = Paragraph::new(tail_text)
                .block(
                    Block::default()
                        .title("Live Tail (Last 10 Messages)")
                        .borders(Borders::ALL),
                )
                .wrap(tui::widgets::Wrap { trim: true })
                .scroll((app.tail_scroll, 0));

            f.render_widget(tail, right_panels[1]);
        }
        AppState::TopicDetail => {
            if let Some(config) = &app.topic_config {
                let items: Vec<ListItem> = config
                    .iter()
                    .enumerate()
                    .map(|(i, (k, v))| {
                        let style = if Some(i) == app.selected_config_index {
                            Style::default()
                                .fg(Color::Black)
                                .bg(Color::White)
                                .add_modifier(Modifier::BOLD)
                        } else {
                            Style::default()
                        };
                        ListItem::new(vec![Spans::from(Span::raw(format!("{}: {}", k, v)))])
                            .style(style)
                    })
                    .collect();

                let config_list = List::new(items)
                    .block(
                        Block::default()
                            .title("Topic Configuration")
                            .borders(Borders::ALL),
                    )
                    .highlight_style(Style::default().add_modifier(Modifier::BOLD))
                    .highlight_symbol("> ");

                f.render_widget(config_list, chunks[2]);
            }
        }
    }
}
