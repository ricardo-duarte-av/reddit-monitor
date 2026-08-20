package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/vartanbeno/go-reddit/v2/reddit"
)

// PostFullID returns the fullname of a post ("t3_<id>"), which is what the
// moderation endpoints expect. Listings normally populate FullID, but fall
// back to building it from the short ID.
func PostFullID(post *reddit.Post) string {
	if post.FullID != "" {
		return post.FullID
	}
	return "t3_" + post.ID
}

// CheckModPermissions reports whether the configured Reddit account moderates
// the target subreddit. Without it, lock and remove return an opaque 403 only
// once someone reacts, which is a long way from the actual cause.
func CheckModPermissions(ctx context.Context, client *reddit.Client, subreddit string) {
	subs, _, err := client.Subreddit.Moderated(ctx, &reddit.ListSubredditOptions{
		ListOptions: reddit.ListOptions{Limit: 100},
	})
	if err != nil {
		log.Printf("WARNING: could not verify moderator status for r/%s: %v\n", subreddit, err)
		return
	}

	for _, sub := range subs {
		if strings.EqualFold(sub.Name, subreddit) {
			log.Printf("Moderator permissions confirmed for r/%s.\n", subreddit)
			return
		}
	}

	log.Printf("WARNING: this account does not moderate r/%s (it moderates %d subreddits). "+
		"Alerts will still be posted, but 🔒 lock and 💣 remove will fail with 403. "+
		"Invite the account as a moderator with 'posts' permission, then accept the invite.\n",
		subreddit, len(subs))
}

// LockPost locks a Reddit thread, preventing further comments.
//
// go-reddit v2.0.1 has no Lock method, so this hits POST api/lock directly
// through the client's request machinery (which handles auth, the user agent
// and rate limiting for us).
func LockPost(ctx context.Context, client *reddit.Client, fullID string) error {
	log.Printf("Locking Reddit post: %s\n", fullID)

	form := url.Values{}
	form.Set("id", fullID)

	req, err := client.NewRequest(http.MethodPost, "api/lock", form)
	if err != nil {
		return fmt.Errorf("failed to build lock request: %v", err)
	}

	if _, err := client.Do(ctx, req, nil); err != nil {
		return fmt.Errorf("failed to lock post %s: %v", fullID, err)
	}

	log.Printf("Post locked successfully: %s\n", fullID)
	return nil
}

// RemovePost removes a Reddit thread as a moderator. The post is hidden from
// the public but stays in the mod queue, so it can be reversed with Approve.
func RemovePost(ctx context.Context, client *reddit.Client, fullID string) error {
	log.Printf("Removing Reddit post: %s\n", fullID)

	if _, err := client.Moderation.Remove(ctx, fullID); err != nil {
		return fmt.Errorf("failed to remove post %s: %v", fullID, err)
	}

	log.Printf("Post removed successfully: %s\n", fullID)
	return nil
}
