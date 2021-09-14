package sessions

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

//session en mémoire global
var (
	sessionsStore Store
)

// Session element lié à session
type Session struct {
	Login           string
	Role            string
	RightLevel      int
	SessionId       string
	Data            map[string]interface{} //donnée attachés générique
	lastActivity    time.Time
	sessionDuration *time.Duration //durée inactivité autorisé du store
}

//Store sessions en mémoire
type Store struct {
	sessions               map[string]*Session
	mu                     sync.Mutex
	defaultSessionDuration time.Duration
}

// InitSessionStore init cach session en mémoire
func InitSessionStore(sessionDuration time.Duration) {
	sessionsStore.sessions = make(map[string]*Session)
	sessionsStore.defaultSessionDuration = sessionDuration
}

//gen id pseudo unique
func genUID() string {
	t := time.Now()
	rand.Seed(time.Now().UTC().UnixNano())
	n1 := rand.Int63()
	n2 := rand.Int63()
	cc := t.UTC().Format(time.RFC3339Nano) + "-" + strconv.FormatInt(n1, 16) + "-" + strconv.FormatInt(n2, 16)

	sha256 := sha256.New()
	sha256.Write([]byte(cc))
	return fmt.Sprintf("%x", sha256.Sum(nil))
}

// get getter session
func (c *Store) get(skey string) *Session {
	c.mu.Lock()
	defer c.mu.Unlock()

	s, exists := c.sessions[skey]
	if len(skey) != 64 || !exists || !s.isValid() {
		s = nil
	} else {
		s.lastActivity = time.Now()
	}

	return s
}

// new create session
func (c *Store) new() *Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	//nouvelle session
	nkey := ""
	for nkey == "" {
		tmp := genUID()
		if _, exists := c.sessions[tmp]; !exists {
			nkey = tmp
		}
	}
	c.sessions[nkey] = &Session{
		SessionId:       nkey,
		Data:            make(map[string]interface{}),
		sessionDuration: &c.defaultSessionDuration,
		lastActivity:    time.Now(),
	}

	return c.sessions[nkey]
}

// Purge supprime les session obsolete
func (c *Store) purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range c.sessions {
		if !v.isValid() {
			delete(c.sessions, k)
		}
	}
}

// remove supprime une session donnée
func (c *Store) remove(sessionId string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.sessions, sessionId)
}

// IsValid test validité session
func (c *Session) isValid() bool {
	if c != nil && time.Since(c.lastActivity) < *c.sessionDuration {
		return true
	}
	return false
}

// Get getter session store principal
func Get(skey string) *Session {
	return sessionsStore.get(skey)
}

// New create session store principal
func New() *Session {
	return sessionsStore.new()
}

// Purge purge session store principal
func Purge() {
	sessionsStore.purge()
}

// Remove supprime une session donnée
func Remove(sessionId string) {
	sessionsStore.remove(sessionId)
}

// List accesseur liste des sessions en cours (token=session)
func List() map[string]*Session {
	return sessionsStore.sessions
}
