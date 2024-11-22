package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
)

// PostgresStore mimics BoltDB's Store structure
type PostgresStore struct {
    db *sql.DB
    mu sync.RWMutex
}

// PostgresTx simulates bolt.Tx behavior
type PostgresTx struct {
    tx        *sql.Tx
    ctx       context.Context
    writeable bool
}

// Bucket simulation for PostgreSQL
type PostgresBucket struct {
    tx         *PostgresTx
    bucketName string
}

// NewPostgresStore creates a new PostgreSQL store
func NewPostgresStore(host, port, dbname, user, password string) (*PostgresStore, error) {
    connStr := fmt.Sprintf(
        "host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
        host, port, dbname, user, password,
    )

    db, err := sql.Open("postgres", connStr)
    if err != nil {
        return nil, err
    }

    // Initialize schema
    if err := initializeSchema(db); err != nil {
        return nil, err
    }

    return &PostgresStore{db: db}, nil
}

// initializeSchema creates necessary tables
func initializeSchema(db *sql.DB) error {
    // Create a generic key-value store table to mimic BoltDB buckets
    _, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS portainer_buckets (
            bucket_name TEXT NOT NULL,
            key BYTEA NOT NULL,
            value BYTEA NOT NULL,
            PRIMARY KEY (bucket_name, key)
        )
    `)
    return err
}

// View implements read-only transaction
func (s *PostgresStore) View(fn func(*PostgresTx) error) error {
    s.mu.RLock()
    defer s.mu.RUnlock()

    tx, err := s.db.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()

    ptx := &PostgresTx{
        tx:        tx,
        ctx:       context.Background(),
        writeable: false,
    }

    if err := fn(ptx); err != nil {
        return err
    }

    return tx.Commit()
}

// Update implements read-write transaction
func (s *PostgresStore) Update(fn func(*PostgresTx) error) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    tx, err := s.db.Begin()
    if err != nil {
        return err
    }

    ptx := &PostgresTx{
        tx:        tx,
        ctx:       context.Background(),
        writeable: true,
    }

    if err := fn(ptx); err != nil {
        tx.Rollback()
        return err
    }

    return tx.Commit()
}

// Bucket creates or retrieves a bucket
func (tx *PostgresTx) Bucket(bucketName []byte) *PostgresBucket {
    return &PostgresBucket{
        tx:         tx,
        bucketName: string(bucketName),
    }
}

// Put stores a key-value pair
func (b *PostgresBucket) Put(key, value []byte) error {
    if !b.tx.writeable {
        return fmt.Errorf("transaction is read-only")
    }

    _, err := b.tx.tx.Exec(`
        INSERT INTO portainer_buckets (bucket_name, key, value)
        VALUES ($1, $2, $3)
        ON CONFLICT (bucket_name, key) DO UPDATE 
        SET value = $3
    `, b.bucketName, key, value)

    return err
}

// Get retrieves a value by key
func (b *PostgresBucket) Get(key []byte) []byte {
    var value []byte
    err := b.tx.tx.QueryRow(`
        SELECT value FROM portainer_buckets 
        WHERE bucket_name = $1 AND key = $2
    `, b.bucketName, key).Scan(&value)

    if err != nil {
        return nil
    }

    return value
}

// Delete removes a key-value pair
func (b *PostgresBucket) Delete(key []byte) error {
    if !b.tx.writeable {
        return fmt.Errorf("transaction is read-only")
    }

    _, err := b.tx.tx.Exec(`
        DELETE FROM portainer_buckets 
        WHERE bucket_name = $1 AND key = $2
    `, b.bucketName, key)

    return err
}

// CreateBucketIfNotExists simulates bolt's bucket creation
func (tx *PostgresTx) CreateBucketIfNotExists(bucketName []byte) (*PostgresBucket, error) {
    // In PostgreSQL, tables are analogous to buckets
    // This is mostly a no-op since we use a single table
    return tx.Bucket(bucketName), nil
}

// Close the database connection
func (s *PostgresStore) Close() error {
    return s.db.Close()
}

// Helper functions for serialization
func serializeToBytes(v interface{}) ([]byte, error) {
    return json.Marshal(v)
}

func deserializeFromBytes(data []byte, v interface{}) error {
    return json.Unmarshal(data, v)
}