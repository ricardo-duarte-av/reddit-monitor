package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/matrix-org/gomatrix"
)

// Reaction keys used as "buttons" under each alert. Stored without variation
// selectors; incoming keys are normalized before comparison.
const (
	LockEmoji = "🔒"
	BombEmoji = "💣"
)

// Reaction keys the bot adds to an alert once the action has succeeded, so the
// outcome is visible on the alert itself instead of as a separate message.
const (
	LockedKey = "locked"
	NukedKey  = "nuked"
)

// normalizeEmoji strips variation selectors and surrounding whitespace so that
// "🔒" and "🔒️" compare equal. Different clients send different forms.
func normalizeEmoji(key string) string {
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		switch r {
		case '\ufe0e', '\ufe0f': // text / emoji variation selectors
			return -1
		}
		return r
	}, key))
}

// SQLiteStore implements gomatrix.Storer, persisting the sync token and filter
// ID in SQLite so that reactions sent while the bot is down are still picked up
// on the next start.
type SQLiteStore struct {
	db    *sql.DB
	mu    sync.Mutex
	rooms map[string]*gomatrix.Room
}

func NewSQLiteStore(db *sql.DB) *SQLiteStore {
	return &SQLiteStore{db: db, rooms: make(map[string]*gomatrix.Room)}
}

func (s *SQLiteStore) get(key string) string {
	var value string
	err := s.db.QueryRow(`SELECT value FROM matrix_state WHERE key = ?`, key).Scan(&value)
	if err != nil {
		return ""
	}
	return value
}

func (s *SQLiteStore) set(key, value string) {
	_, err := s.db.Exec(
		`INSERT INTO matrix_state (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, value,
	)
	if err != nil {
		log.Printf("Failed to persist matrix state %q: %v\n", key, err)
	}
}

func (s *SQLiteStore) SaveFilterID(userID, filterID string) { s.set("filter_id:"+userID, filterID) }
func (s *SQLiteStore) LoadFilterID(userID string) string    { return s.get("filter_id:" + userID) }
func (s *SQLiteStore) SaveNextBatch(userID, token string)   { s.set("next_batch:"+userID, token) }
func (s *SQLiteStore) LoadNextBatch(userID string) string   { return s.get("next_batch:" + userID) }

// Rooms are only used for in-memory state tracking by the syncer; there is no
// need to persist them across restarts.
func (s *SQLiteStore) SaveRoom(room *gomatrix.Room) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rooms[room.ID] = room
}

func (s *SQLiteStore) LoadRoom(roomID string) *gomatrix.Room {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rooms[roomID]
}

// ResolveMatrixUserID asks the homeserver who the access token belongs to.
// The config only holds a localpart, and the syncer needs the full MXID to
// recognise the bot's own events.
func ResolveMatrixUserID(client *gomatrix.Client) (string, error) {
	var resp struct {
		UserID string `json:"user_id"`
	}
	err := client.MakeRequest("GET", client.BuildURL("account", "whoami"), nil, &resp)
	if err != nil {
		return "", fmt.Errorf("whoami request failed: %v", err)
	}
	if resp.UserID == "" {
		return "", fmt.Errorf("whoami returned an empty user ID")
	}
	return resp.UserID, nil
}

// SendMatrixMessage sends a formatted message to the configured room and
// returns the event ID of the message that was sent.
func (b *Bot) SendMatrixMessage(plainText, formattedHTML string) (string, error) {
	log.Printf("Sending message to Matrix room %s\n", b.config.Matrix.RoomID)

	resp, err := b.matrix.SendMessageEvent(b.config.Matrix.RoomID, "m.room.message", map[string]interface{}{
		"msgtype":        "m.text",
		"body":           plainText,
		"format":         "org.matrix.custom.html",
		"formatted_body": formattedHTML,
	})
	if err != nil {
		return "", fmt.Errorf("failed to send Matrix message: %v", err)
	}

	log.Printf("Message sent to Matrix room successfully (event %s).\n", resp.EventID)
	return resp.EventID, nil
}

// SendReaction annotates an existing event with an emoji.
func (b *Bot) SendReaction(targetEventID, key string) error {
	_, err := b.matrix.SendMessageEvent(b.config.Matrix.RoomID, "m.reaction", map[string]interface{}{
		"m.relates_to": map[string]interface{}{
			"rel_type": "m.annotation",
			"event_id": targetEventID,
			"key":      key,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to send %s reaction: %v", key, err)
	}
	return nil
}

// SendHorizontalLine sends a separator between alerts.
func (b *Bot) SendHorizontalLine() error {
	_, err := b.matrix.SendMessageEvent(b.config.Matrix.RoomID, "m.room.message", map[string]interface{}{
		"msgtype":        "m.text",
		"body":           "------------",
		"format":         "org.matrix.custom.html",
		"formatted_body": "<hr>",
	})
	if err != nil {
		return fmt.Errorf("failed to send horizontal line: %v", err)
	}
	return nil
}

// SendNotice posts a short status line back into the room (action confirmations
// and failures). Sent as m.notice so clients render it differently from alerts.
func (b *Bot) SendNotice(text string) {
	_, err := b.matrix.SendMessageEvent(b.config.Matrix.RoomID, "m.room.message", map[string]interface{}{
		"msgtype": "m.notice",
		"body":    text,
	})
	if err != nil {
		log.Printf("Failed to send notice %q: %v\n", text, err)
	}
}

// PostAlert sends the alert for a post, records the mapping from Matrix event
// to Reddit post, and adds the 🔒 / 💣 reaction buttons.
func (b *Bot) PostAlert(author, title, body, subredditPrefixed, postID, postFullID string) error {
	plainText, formattedHTML := FormatMatrixMessage(author, title, body, subredditPrefixed, postID)

	eventID, err := b.SendMatrixMessage(plainText, formattedHTML)
	if err != nil {
		return err
	}

	// Record the mapping before adding the buttons, so a reaction that arrives
	// immediately can always be resolved back to a post.
	if err := RecordAlert(b.db, eventID, postID, postFullID, subredditPrefixed); err != nil {
		return fmt.Errorf("failed to record alert for post %s: %v", postID, err)
	}

	for _, key := range []string{LockEmoji, BombEmoji} {
		if err := b.SendReaction(eventID, key); err != nil {
			// Non-fatal: the alert is already up, the button is just missing.
			log.Printf("Error adding %s button to event %s: %v\n", key, eventID, err)
		}
	}

	return nil
}

// HandleReaction acts on reactions added by other users to one of the bot's
// alerts: 🔒 locks the Reddit thread, 💣 removes it as a moderator.
func (b *Bot) HandleReaction(ev *gomatrix.Event) {
	if ev.RoomID != b.config.Matrix.RoomID {
		return
	}
	// Ignore the bot's own reactions, which are the buttons themselves.
	if ev.Sender == b.matrixUserID {
		return
	}

	relatesTo, ok := ev.Content["m.relates_to"].(map[string]interface{})
	if !ok {
		return
	}
	if relType, _ := relatesTo["rel_type"].(string); relType != "m.annotation" {
		return
	}

	targetEventID, _ := relatesTo["event_id"].(string)
	key, _ := relatesTo["key"].(string)
	if targetEventID == "" || key == "" {
		return
	}

	var action string
	switch normalizeEmoji(key) {
	case LockEmoji:
		action = ActionLock
	case BombEmoji:
		action = ActionRemove
	default:
		return // some other emoji, not ours
	}

	alert, err := LookupAlert(b.db, targetEventID)
	if err != nil {
		log.Printf("Error looking up alert for event %s: %v\n", targetEventID, err)
		return
	}
	if alert == nil {
		return // a reaction to something that isn't one of our alerts
	}

	log.Printf("Reaction %s from %s on post %s -> %s\n", key, ev.Sender, alert.PostID, action)
	b.runAction(action, alert, ev.Sender)
}

// runAction claims the action in the database first so that two users reacting
// at the same time only trigger one Reddit call, then performs it. If the
// Reddit call fails the claim is released so a later reaction can retry.
func (b *Bot) runAction(action string, alert *Alert, sender string) {
	claimed, err := ClaimAction(b.db, alert.EventID, action)
	if err != nil {
		log.Printf("Error claiming %s for post %s: %v\n", action, alert.PostID, err)
		return
	}
	if !claimed {
		log.Printf("Action %s already performed for post %s, ignoring.\n", action, alert.PostID)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var actionErr error
	var doneKey string
	switch action {
	case ActionLock:
		doneKey = LockedKey
		actionErr = LockPost(ctx, b.reddit, alert.PostFullID)
	case ActionRemove:
		doneKey = NukedKey
		actionErr = RemovePost(ctx, b.reddit, alert.PostFullID)
	}

	postURL := fmt.Sprintf("https://old.reddit.com/%s/comments/%s", alert.Subreddit, alert.PostID)

	if actionErr != nil {
		log.Printf("Failed to %s post %s: %v\n", action, alert.PostID, actionErr)
		if err := ReleaseAction(b.db, alert.EventID, action); err != nil {
			log.Printf("Error releasing %s claim for post %s: %v\n", action, alert.PostID, err)
		}
		b.SendNotice(fmt.Sprintf("⚠️ Failed to %s %s (requested by %s): %v", action, postURL, sender, actionErr))
		return
	}

	// Mark the outcome on the alert itself rather than posting a separate
	// message into the room.
	if err := b.SendReaction(alert.EventID, doneKey); err != nil {
		log.Printf("Error marking event %s as %s: %v\n", alert.EventID, doneKey, err)
	}
	log.Printf("Post %s %s (requested by %s): %s\n", alert.PostID, doneKey, sender, postURL)
}

// RunSyncLoop keeps the Matrix sync running until the context is cancelled,
// reconnecting after fatal sync errors.
func (b *Bot) RunSyncLoop(ctx context.Context) {
	for ctx.Err() == nil {
		if err := b.matrix.Sync(); err != nil {
			log.Printf("Matrix sync error: %v\n", err)
		}
		if ctx.Err() != nil {
			return
		}
		log.Println("Matrix sync stopped, reconnecting in 10s...")
		select {
		case <-time.After(10 * time.Second):
		case <-ctx.Done():
			return
		}
	}
}
