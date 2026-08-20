package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/matrix-org/gomatrix"
	_ "github.com/mattn/go-sqlite3" // Import SQLite driver
	openai "github.com/sashabaranov/go-openai"
	"github.com/vartanbeno/go-reddit/v2/reddit"
	"gopkg.in/yaml.v2"
)

// Actions that can be triggered by reacting to an alert.
const (
	ActionLock   = "lock"
	ActionRemove = "remove"
)

// defaultPollInterval is used when bot.poll_interval is not set in the config.
const defaultPollInterval = 2 * time.Minute

// Config structure maps to config.yaml
type Config struct {
	Reddit struct {
		ClientID     string `yaml:"client_id"`
		ClientSecret string `yaml:"client_secret"`
		Username     string `yaml:"username"`
		Password     string `yaml:"password"`
	} `yaml:"reddit"`
	OpenAI struct {
		APIKey string `yaml:"api_key"`
	} `yaml:"openai"`
	Matrix struct {
		Server   string `yaml:"server"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		Token    string `yaml:"token"`
		RoomID   string `yaml:"room_id"`
	} `yaml:"matrix"`
	Bot struct {
		Subreddit    string `yaml:"subreddit"`
		Prompt       string `yaml:"prompt"`
		PollInterval string `yaml:"poll_interval"`
	} `yaml:"bot"`
	SQLite struct {
		DBPath string `yaml:"db_path"`
	} `yaml:"sqlite"`
}

// PollInterval returns how often to check the subreddit for new posts.
func (c *Config) PollInterval() time.Duration {
	if c.Bot.PollInterval == "" {
		return defaultPollInterval
	}
	d, err := time.ParseDuration(c.Bot.PollInterval)
	if err != nil {
		log.Printf("Invalid bot.poll_interval %q, falling back to %s: %v\n", c.Bot.PollInterval, defaultPollInterval, err)
		return defaultPollInterval
	}
	return d
}

// Bot holds the long-lived clients and state shared by the poller and the
// Matrix sync loop.
type Bot struct {
	config       *Config
	db           *sql.DB
	reddit       *reddit.Client
	matrix       *gomatrix.Client
	matrixUserID string
}

// Alert is a Matrix message the bot posted about a Reddit thread, and the
// thread it points at.
type Alert struct {
	EventID    string
	PostID     string
	PostFullID string
	Subreddit  string
}

// LoadConfig reads and loads the configuration from the config.yaml file
func LoadConfig(filePath string) (*Config, error) {
	log.Println("Reading configuration from file:", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("unable to open config file: %v", err)
	}
	defer file.Close()

	var config Config
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("unable to decode config file: %v", err)
	}

	log.Println("Configuration loaded successfully.")
	return &config, nil
}

// InitializeSQLite sets up the SQLite database and ensures the necessary tables exist
func InitializeSQLite(dbPath string) (*sql.DB, error) {
	log.Println("Initializing SQLite database at path:", dbPath)

	// The sync goroutine and the poll workers write concurrently now, so ask
	// the driver to wait on a held lock instead of failing immediately.
	dsn := dbPath
	if !strings.Contains(dsn, "?") {
		dsn += "?_busy_timeout=5000&_journal_mode=WAL"
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %v", err)
	}

	query := `
	CREATE TABLE IF NOT EXISTS processed_posts (
		id TEXT PRIMARY KEY,
		processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS alert_messages (
		event_id     TEXT PRIMARY KEY,
		post_id      TEXT NOT NULL,
		post_full_id TEXT NOT NULL,
		subreddit    TEXT NOT NULL,
		locked       INTEGER NOT NULL DEFAULT 0,
		removed      INTEGER NOT NULL DEFAULT 0,
		created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS matrix_state (
		key   TEXT PRIMARY KEY,
		value TEXT NOT NULL
	);
	`
	_, err = db.Exec(query)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}

	log.Println("SQLite database initialized successfully.")
	return db, nil
}

// IsPostProcessed checks if a Reddit post has already been processed
func IsPostProcessed(db *sql.DB, postID string) (bool, error) {
	var exists bool
	query := `SELECT COUNT(1) FROM processed_posts WHERE id = ?`
	err := db.QueryRow(query, postID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to query database: %v", err)
	}
	return exists, nil
}

// MarkPostAsProcessed records a processed Reddit post ID in the database
func MarkPostAsProcessed(db *sql.DB, postID string) error {
	query := `INSERT INTO processed_posts (id) VALUES (?)`
	_, err := db.Exec(query, postID)
	if err != nil {
		return fmt.Errorf("failed to insert post ID: %v", err)
	}
	return nil
}

// RecordAlert stores the mapping from a Matrix alert event to the Reddit thread
// it is about, so that reactions can be resolved back to a post later.
func RecordAlert(db *sql.DB, eventID, postID, postFullID, subreddit string) error {
	query := `INSERT OR REPLACE INTO alert_messages (event_id, post_id, post_full_id, subreddit) VALUES (?, ?, ?, ?)`
	_, err := db.Exec(query, eventID, postID, postFullID, subreddit)
	if err != nil {
		return fmt.Errorf("failed to insert alert message: %v", err)
	}
	return nil
}

// LookupAlert finds the alert for a Matrix event ID. It returns (nil, nil) when
// the event is not one of the bot's alerts.
func LookupAlert(db *sql.DB, eventID string) (*Alert, error) {
	alert := &Alert{EventID: eventID}
	query := `SELECT post_id, post_full_id, subreddit FROM alert_messages WHERE event_id = ?`
	err := db.QueryRow(query, eventID).Scan(&alert.PostID, &alert.PostFullID, &alert.Subreddit)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query alert message: %v", err)
	}
	return alert, nil
}

// actionColumn maps an action name to the column tracking whether it ran.
func actionColumn(action string) (string, error) {
	switch action {
	case ActionLock:
		return "locked", nil
	case ActionRemove:
		return "removed", nil
	default:
		return "", fmt.Errorf("unknown action: %s", action)
	}
}

// ClaimAction atomically marks an action as done for an alert. It returns false
// if the action had already been claimed, so that several people reacting with
// the same emoji only trigger one Reddit call.
func ClaimAction(db *sql.DB, eventID, action string) (bool, error) {
	column, err := actionColumn(action)
	if err != nil {
		return false, err
	}

	query := fmt.Sprintf(`UPDATE alert_messages SET %s = 1 WHERE event_id = ? AND %s = 0`, column, column)
	res, err := db.Exec(query, eventID)
	if err != nil {
		return false, fmt.Errorf("failed to claim action %s: %v", action, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to read claim result for %s: %v", action, err)
	}
	return affected > 0, nil
}

// ReleaseAction undoes a claim after the Reddit call failed, so a later
// reaction can retry it.
func ReleaseAction(db *sql.DB, eventID, action string) error {
	column, err := actionColumn(action)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`UPDATE alert_messages SET %s = 0 WHERE event_id = ?`, column)
	if _, err := db.Exec(query, eventID); err != nil {
		return fmt.Errorf("failed to release action %s: %v", action, err)
	}
	return nil
}

// AnalyzeText sends a Reddit post's title and body to OpenAI for analysis using the o4-mini model
func AnalyzeText(apiKey, prompt, postTitle, postBody string) (bool, error) {
	client := openai.NewClient(apiKey)

	fullPost := fmt.Sprintf("Title: %s\nBody: %s", postTitle, postBody)
	fullPrompt := fmt.Sprintf(prompt, fullPost)

	log.Printf("Full prompt sent to OpenAI: %s\n", fullPrompt)
	log.Println("Sending text to OpenAI for analysis...")

	resp, err := client.CreateChatCompletion(context.Background(), openai.ChatCompletionRequest{
		Model: "o4-mini",
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleSystem,
				Content: "You are an assistant designed to analyze text. Respond only with 'true' or 'false' based on the analysis.",
			},
			{
				Role:    openai.ChatMessageRoleUser,
				Content: fullPrompt,
			},
		},
		MaxCompletionTokens: 500,
	})
	if err != nil {
		return false, fmt.Errorf("OpenAI API error: %v", err)
	}

	if len(resp.Choices) == 0 || resp.Choices[0].Message.Content == "" {
		log.Println("OpenAI response is empty: No valid content returned.")
		return false, fmt.Errorf("OpenAI response is empty or invalid")
	}

	responseText := resp.Choices[0].Message.Content
	log.Printf("OpenAI parsed response: %s\n", responseText)
	return responseText == "true", nil
}

// FormatMatrixMessage generates a Matrix message with Reddit post details, including the body
func FormatMatrixMessage(userName, postTitle, postBody, subreddit, postID string) (string, string) {
	plainText := fmt.Sprintf(
		`💾 Potential ROM request detected!

User /u/%s sent this post:

Title: %s

Body:
%s

It seems to be asking for ROMs. Could someone investigate?

React with %s to lock the thread, or %s to remove it.

🔗 View Post on Reddit: https://old.reddit.com/%s/comments/%s`,
		userName, postTitle, postBody, LockEmoji, BombEmoji, subreddit, postID,
	)

	formattedHTML := fmt.Sprintf(
		`<b>💾 Potential ROM request detected!</b><br><br>
<b>User:</b> /u/%s<br>
<b>Title:</b> %s<br>
<b>Body:</b><br>
%s<br><br>
It seems to be asking for ROMs. Could someone investigate?<br><br>
React with %s to lock the thread, or %s to remove it.<br><br>
<a href="https://old.reddit.com/%s/comments/%s">🔗 View Post on Reddit</a>`,
		userName, postTitle, postBody, LockEmoji, BombEmoji, subreddit, postID,
	)

	return plainText, formattedHTML
}

// processPostWorker analyzes posts off the channel and alerts on ROM requests.
func (b *Bot) processPostWorker(posts <-chan reddit.Post, wg *sync.WaitGroup) {
	defer wg.Done()

	for post := range posts {
		log.Printf("Worker processing post: %s by /u/%s\n", post.ID, post.Author)

		processed, err := IsPostProcessed(b.db, post.ID)
		if err != nil {
			log.Printf("Error checking post ID: %s. Error: %v\n", post.ID, err)
			continue
		}
		if processed {
			continue
		}

		isROMRequest, err := AnalyzeText(b.config.OpenAI.APIKey, b.config.Bot.Prompt, post.Title, post.Body)
		if err != nil {
			log.Printf("Error analyzing text for post ID: %s. Error: %v\n", post.ID, err)
			continue
		}

		if isROMRequest {
			// Send the horizontal line first so the alert itself is the last
			// event in the room: mobile push previews show the message body
			// rather than the separator.
			if err := b.SendHorizontalLine(); err != nil {
				log.Printf("Error sending horizontal line for post ID: %s. Error: %v\n", post.ID, err)
			}

			err := b.PostAlert(post.Author, post.Title, post.Body, post.SubredditNamePrefixed, post.ID, PostFullID(&post))
			if err != nil {
				log.Printf("Error posting alert for post ID: %s. Error: %v\n", post.ID, err)
			}
		}

		err = MarkPostAsProcessed(b.db, post.ID)
		if err != nil {
			log.Printf("Error marking post as processed: %s. Error: %v\n", post.ID, err)
		} else {
			log.Printf("Post marked as processed: %s\n", post.ID)
		}
	}
}

// PollSubreddit fetches the newest posts once and runs them through the workers.
func (b *Bot) PollSubreddit(ctx context.Context) {
	log.Printf("Fetching new posts from subreddit: %s\n", b.config.Bot.Subreddit)
	posts, _, err := b.reddit.Subreddit.NewPosts(ctx, b.config.Bot.Subreddit, &reddit.ListOptions{
		Limit: 10,
	})
	if err != nil {
		log.Printf("Failed to fetch posts: %v\n", err)
		return
	}

	postChannel := make(chan reddit.Post, len(posts))
	var wg sync.WaitGroup

	numWorkers := 3
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go b.processPostWorker(postChannel, &wg)
	}

	for _, post := range posts {
		postChannel <- *post
	}

	close(postChannel)
	wg.Wait()

	log.Println("Poll cycle finished.")
}

func main() {
	log.Println("Starting Reddit Monitor Bot...")

	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	db, err := InitializeSQLite(config.SQLite.DBPath)
	if err != nil {
		log.Fatalf("Error initializing SQLite: %v", err)
	}
	defer db.Close()

	log.Println("Logging in to Reddit...")
	redditClient, err := reddit.NewClient(reddit.Credentials{
		ID:       config.Reddit.ClientID,
		Secret:   config.Reddit.ClientSecret,
		Username: config.Reddit.Username,
		Password: config.Reddit.Password,
	})
	if err != nil {
		log.Fatalf("Failed to create Reddit client: %v", err)
	}
	log.Println("Logged in to Reddit successfully.")

	CheckModPermissions(context.Background(), redditClient, config.Bot.Subreddit)

	log.Println("Connecting to Matrix...")
	matrixClient, err := gomatrix.NewClient(config.Matrix.Server, "", config.Matrix.Token)
	if err != nil {
		log.Fatalf("Failed to create Matrix client: %v", err)
	}

	// The config holds a localpart, but the syncer needs the full MXID to tell
	// the bot's own reactions apart from everyone else's.
	matrixUserID, err := ResolveMatrixUserID(matrixClient)
	if err != nil {
		log.Fatalf("Failed to resolve Matrix user ID: %v", err)
	}
	log.Printf("Matrix user ID: %s\n", matrixUserID)

	store := NewSQLiteStore(db)
	matrixClient.SetCredentials(matrixUserID, config.Matrix.Token)
	matrixClient.Store = store
	syncer := gomatrix.NewDefaultSyncer(matrixUserID, store)
	matrixClient.Syncer = syncer

	bot := &Bot{
		config:       config,
		db:           db,
		reddit:       redditClient,
		matrix:       matrixClient,
		matrixUserID: matrixUserID,
	}

	syncer.OnEventType("m.reaction", bot.HandleReaction)

	if _, err := matrixClient.JoinRoom(config.Matrix.RoomID, "", nil); err != nil {
		log.Printf("Could not join room %s (already joined?): %v\n", config.Matrix.RoomID, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("Starting Matrix sync loop...")
	var syncWG sync.WaitGroup
	syncWG.Add(1)
	go func() {
		defer syncWG.Done()
		bot.RunSyncLoop(ctx)
	}()

	interval := config.PollInterval()
	log.Printf("Polling /r/%s every %s.\n", config.Bot.Subreddit, interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	bot.PollSubreddit(ctx)

	for {
		select {
		case <-ticker.C:
			bot.PollSubreddit(ctx)
		case <-ctx.Done():
			log.Println("Shutdown signal received, stopping...")
			matrixClient.StopSync()
			syncWG.Wait()
			log.Println("Reddit Monitor Bot stopped.")
			return
		}
	}
}
