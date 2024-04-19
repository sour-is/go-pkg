package libsqlembed

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
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
	sql.Register("libsql+embed", &db{conns: make(map[string]*connector)})
}

type db struct {
	conns map[string]*connector
	mu    sync.RWMutex
}

type connector struct {
	*libsql.Connector
	dsn       string
	dir       string
	driver    *db
	removeDir bool
}

var _ io.Closer = (*connector)(nil)

func (c *connector) Close() error {
	log.Println("closing db connection", c.dir)
	defer log.Println("closed db connection", c.dir)

	c.driver.mu.Lock()
	delete(c.driver.conns, c.dsn)
	c.driver.mu.Unlock()

	if c.removeDir {
		defer os.RemoveAll(c.dir)
	}

	log.Println("sync db")
	if err := c.Connector.Sync(); err != nil {
		return fmt.Errorf("syncing database: %w", err)
	}

	return c.Connector.Close()
}

func (db *db) OpenConnector(dsn string) (driver.Connector, error) {
	// log.Println("connector", dsn)
	if dsn == "" {
		return nil, fmt.Errorf("no dsn")
	}

	if c, ok := func() (*connector, bool) {
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

	if refresh, err := strconv.ParseInt(u.Query().Get("refresh"), 10, 64); err == nil {
		log.Println("refresh: ", refresh)
		opts = append(opts, libsql.WithSyncInterval(time.Duration(refresh)*time.Minute))
	}

	if readWrite, err := strconv.ParseBool(u.Query().Get("readYourWrites")); err == nil {
		log.Println("read your writes: ", readWrite)
		opts = append(opts, libsql.WithReadYourWrites(readWrite))
	}
	if key := u.Query().Get("key"); key != "" {
		opts = append(opts, libsql.WithEncryption(key))
	}

	var dir string
	var removeDir bool
	if dir = u.Query().Get("store"); dir == "" {
		removeDir = true
		dir, err = os.MkdirTemp("", "libsql-*")
		log.Println("creating temporary directory:", dir)
		if err != nil {
			return nil, fmt.Errorf("creating temporary directory: %w", err)
		}
	} else {
		stat, err := os.Stat(dir)
		if errors.Is(err, os.ErrNotExist) {
			if err = os.MkdirAll(dir, 0700); err != nil {
				return nil, err
			}
		} else {
			if !stat.IsDir() {
				return nil, fmt.Errorf("store not directory")
			}
		}
	}

	dbPath := filepath.Join(dir, dbname)

	c, err := libsql.NewEmbeddedReplicaConnector(
		dbPath,
		primary.String(),
		opts...)
	if err != nil {
		return nil, fmt.Errorf("creating connector: %w", err)
	}

	log.Println("sync db")
	if err := c.Sync(); err != nil {
		return nil, fmt.Errorf("syncing database: %w", err)
	}
	connector := &connector{c, dsn, dir, db, removeDir}
	db.conns[dsn] = connector

	return connector, nil
}

func (db *db) Open(dsn string) (driver.Conn, error) {
	log.Println("open", dsn)

	c, err := db.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}
