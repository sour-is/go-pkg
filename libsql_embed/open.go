package libsqlembed

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tursodatabase/go-libsql"
)

func init() {
	sql.Register("libsql+embed", &db{})
}

type db struct {
	conns map[string]connector
	mu    sync.RWMutex
}

type connector struct {
	*libsql.Connector
	dsn    string
	dir    string
	driver *db
}

func (c *connector) Close() error {
	c.driver.mu.Lock()
	delete(c.driver.conns, c.dsn)
	c.driver.mu.Unlock()

	defer os.RemoveAll(c.dir)

	if err := c.Connector.Sync(); err != nil {
		return fmt.Errorf("syncing database: %w", err)
	}

	return c.Connector.Close()
}

func (db *db) OpenConnector(dsn string) (driver.Connector, error) {
	if c, ok := func() (connector, bool) {
		db.mu.RLock()
		defer db.mu.RUnlock()
		c, ok := db.conns[dsn]
		return c, ok
	}(); ok {
		return c, nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	var primary url.URL
	primary.Scheme = strings.TrimSuffix(u.Scheme, "+embed")
	primary.Host = u.Host

	dbname, _, _ := strings.Cut(u.Host, ".")

	authToken := u.Query().Get("authToken")
	if authToken == "" {
		return nil, fmt.Errorf("missing authToken")
	}

	opts := []libsql.Option{
		libsql.WithAuthToken(authToken),
	}

	if refresh, err := strconv.ParseInt(u.Query().Get("refresh"),10,64); err == nil {
		opts = append(opts, libsql.WithSyncInterval(time.Duration(refresh)*time.Minute))
	}

	if readWrite, err := strconv.ParseBool(u.Query().Get("readYourWrites")); err == nil {
		opts = append(opts, libsql.WithReadYourWrites(readWrite))
	}
	if key := u.Query().Get("key"); key != "" {
		opts = append(opts, libsql.WithEncryption(key))
	}
	
	dir, err := os.MkdirTemp("", "libsql-*")
	if err != nil {
		return nil, fmt.Errorf("creating temporary directory: %w", err)
	}

	dbPath := filepath.Join(dir, dbname)

	c, err := libsql.NewEmbeddedReplicaConnector(
		dbPath,
		primary.String(),
		opts...)
	if err != nil {
		return nil, fmt.Errorf("creating connector: %w", err)
	}

	connector := connector{c, dsn, dir, db}
	db.conns[dsn] = connector

	return connector, nil
}

func (db *db) Open(dsn string) (driver.Conn, error) {
	c, err := db.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}
