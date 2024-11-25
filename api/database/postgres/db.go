package postgres

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	portainer "github.com/portainer/portainer/api"
	"github.com/rs/zerolog/log"
)

const (
	// Database configuration constants
	DatabaseDriverName = "postgres"
	DatabaseMaxOpen   = 25
	DatabaseMaxIdle   = 25
	DatabaseTimeout   = 5 * time.Minute

	// Metadata table names
	EncryptedMetadataTable   = "encrypted_metadata"
	UnencryptedMetadataTable = "unencrypted_metadata"
)

var (
	ErrHaveEncryptedAndUnencrypted = errors.New("portainer has detected both an encrypted and un-encrypted database and cannot start")
	ErrHaveEncryptedWithNoKey      = errors.New("the portainer database is encrypted, but no secret was loaded")
	ErrNoConnection               = errors.New("database connection is not initialized")
)

// DbConnection represents a PostgreSQL database connection
type DbConnection struct {
	ConnectionString string
	Path            string
	EncryptionKey   []byte
	isEncrypted     bool
	ctx             context.Context
	cancelFunc      context.CancelFunc

	*sqlx.DB
}

// NewConnection creates a new database connection
func NewConnection(connectionString string, encryptionKey []byte) (*DbConnection, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	conn := &DbConnection{
		ConnectionString: connectionString,
		Path:            connectionString,
		EncryptionKey:   encryptionKey,
		ctx:             ctx,
		cancelFunc:      cancel,
	}

	if err := conn.Open(); err != nil {
		cancel()
		return nil, err
	}

	return conn, nil
}

// GetStorePath returns the connection string path
func (connection *DbConnection) GetStorePath() string {
	return connection.Path
}

func (connection *DbConnection) SetEncrypted(flag bool) {
	connection.isEncrypted = flag
}

// IsEncryptedStore returns true if the database is encrypted
func (connection *DbConnection) IsEncryptedStore() bool {
	return connection.getEncryptionKey() != nil
}
func (connection *DbConnection) ConvertToKey(key int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(key))
    return b
}
// NeedsEncryptionMigration checks if database needs encryption migration
func (connection *DbConnection) NeedsEncryptionMigration() (bool, error) {
	if connection.DB == nil {
		return false, ErrNoConnection
	}

	checkTableExists := func(tableName string) (bool, error) {
		var exists bool
		query := `
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'public' 
				AND table_name = $1
			);`
		err := connection.QueryRowx(query, tableName).Scan(&exists)
		return exists, err
	}

	haveUnencrypted, err := checkTableExists(UnencryptedMetadataTable)
	if err != nil {
		return false, fmt.Errorf("failed to check unencrypted table: %w", err)
	}

	haveEncrypted, err := checkTableExists(EncryptedMetadataTable)
	if err != nil {
		return false, fmt.Errorf("failed to check encrypted table: %w", err)
	}

	switch {
	case haveUnencrypted && haveEncrypted:
		return false, ErrHaveEncryptedAndUnencrypted
	case haveUnencrypted && connection.EncryptionKey != nil:
		return true, nil
	case haveEncrypted && connection.EncryptionKey == nil:
		return false, ErrHaveEncryptedWithNoKey
	default:
		return false, nil
	}
}

// Open opens and initializes the PostgreSQL database connection
func (connection *DbConnection) Open() error {
	log.Info().Str("connection", connection.ConnectionString).Msg("connecting to PostgreSQL database")

	db, err := sqlx.Connect(DatabaseDriverName, connection.ConnectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(DatabaseMaxOpen)
	db.SetMaxIdleConns(DatabaseMaxIdle)
	db.SetConnMaxLifetime(DatabaseTimeout)

	// Verify connection
	if err := db.PingContext(connection.ctx); err != nil {
		return fmt.Errorf("failed to verify database connection: %w", err)
	}

	connection.DB = db
	return nil
}

// Close closes the PostgreSQL database connection
func (connection *DbConnection) Close() error {
	log.Info().Msg("closing PostgreSQL connection")

	if connection.cancelFunc != nil {
		connection.cancelFunc()
	}

	if connection.DB != nil {
		return connection.DB.Close()
	}

	return nil
}

// UpdateTx executes the given function within a transaction
func (connection *DbConnection) UpdateTx(fn func(portainer.Transaction) error) error {
	if connection.DB == nil {
		return ErrNoConnection
	}

	tx, err := connection.BeginTxx(connection.ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	pgTx := &DbTransaction{
		conn: connection,
		tx:   tx,
	}

	if err := fn(pgTx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error().Err(rbErr).Msg("failed to rollback transaction")
		}
		return err
	}

	return tx.Commit()
}

// ViewTx executes a read-only transaction
func (connection *DbConnection) ViewTx(fn func(portainer.Transaction) error) error {
	return connection.UpdateTx(fn) // PostgreSQL doesn't require special handling for read-only transactions
}

// GetNextIdentifier retrieves the next available ID for a table
func (connection *DbConnection) GetNextIdentifier(tableName string) int {
	var nextID int
	query := fmt.Sprintf("SELECT COALESCE(MAX(id), 0) + 1 FROM %s", tableName)
	
	err := connection.GetContext(connection.ctx, &nextID, query)
	if err != nil {
		log.Error().Err(err).Str("table", tableName).Msg("failed to get next identifier")
		return 1 // Return 1 as fallback for first entry
	}
	
	return nextID
}

// BackupTo exports the database to a writer
func (connection *DbConnection) BackupTo(w io.Writer) error {
	if connection.DB == nil {
		return ErrNoConnection
	}

	rows, err := connection.QueryxContext(connection.ctx, `
		SELECT 
			table_schema,
			table_name,
			column_name,
			data_type
		FROM 
			information_schema.columns
		WHERE 
			table_schema = 'public'
		ORDER BY 
			table_name, ordinal_position
	`)
	if err != nil {
		return fmt.Errorf("failed to query schema: %w", err)
	}
	defer rows.Close()

	schemas := make(map[string][]string)
	for rows.Next() {
		var schema, table, column, dataType string
		if err := rows.Scan(&schema, &table, &column, &dataType); err != nil {
			return fmt.Errorf("failed to scan schema row: %w", err)
		}
		schemas[table] = append(schemas[table], fmt.Sprintf("%s %s", column, dataType))
	}

	// Write schema information
	for table, columns := range schemas {
		fmt.Fprintf(w, "Table: %s\nColumns:\n", table)
		for _, col := range columns {
			fmt.Fprintf(w, "  %s\n", col)
		}
		fmt.Fprintln(w, "---")
	}

	return nil
}

func (connection *DbConnection) getEncryptionKey() []byte {
	if !connection.isEncrypted {
		return nil
	}
	return connection.EncryptionKey
}

// CreateObject creates a new object in the specified table
func (connection *DbConnection) CreateObject(bucketName string, fn func(uint64) (int, interface{})) error {
	return connection.UpdateTx(func(tx portainer.Transaction) error {
		nextID := uint64(connection.GetNextIdentifier(bucketName))
		id, obj := fn(nextID)
		return tx.CreateObjectWithId(bucketName, id, obj)
	})
}

// CreateObjectWithId creates an object with a specified ID
func (connection *DbConnection) CreateObjectWithId(bucketName string, id int, obj any) error {
	return connection.UpdateTx(func(tx portainer.Transaction) error {
		return tx.CreateObjectWithId(bucketName, id, obj)
	})
}

// CreateObjectWithStringId creates an object with a string ID
func (connection *DbConnection) CreateObjectWithStringId(bucketName string, id []byte, obj any) error {
	return connection.UpdateTx(func(tx portainer.Transaction) error {
		return tx.CreateObjectWithStringId(bucketName, id, obj)
	})
}

// MarshalObject converts an object to JSON
// func (connection *DbConnection) MarshalObject(obj any) ([]byte, error) {
// 	return json.Marshal(obj)
// }

// // UnmarshalObject converts JSON to an object
// func (connection *DbConnection) UnmarshalObject(data []byte, obj any) error {
// 	return json.Unmarshal(data, obj)
// }

// Other methods would be implemented similarly...

// GetObject retrieves an object from a table
func (connection *DbConnection) GetObject(bucketName string, key []byte, object any) error {
	return connection.ViewTx(func(tx portainer.Transaction) error {
		return tx.GetObject(bucketName, key, object)
	})
}

// UpdateObject updates an object in a table
func (connection *DbConnection) UpdateObject(bucketName string, key []byte, object any) error {
	return connection.UpdateTx(func(tx portainer.Transaction) error {
		return tx.UpdateObject(bucketName, key, object)
	})
}

// DeleteObject removes an object from a table
func (connection *DbConnection) DeleteObject(bucketName string, key []byte) error {
	return connection.UpdateTx(func(tx portainer.Transaction) error {
		return tx.DeleteObject(bucketName, key)
	})
}

// GetAll retrieves all objects from a table
func (connection *DbConnection) GetAll(bucketName string, obj any, appendFn func(o any) (any, error)) error {
	return connection.ViewTx(func(tx portainer.Transaction) error {
		return tx.GetAll(bucketName, obj, appendFn)
	})
}

// BackupMetadata retrieves sequence/identity information
func (connection *DbConnection) BackupMetadata() (map[string]any, error) {
	metadata := make(map[string]any)

	rows, err := connection.DB.Query(`
		SELECT tablename, pg_get_serial_sequence(tablename, 'id') as seq
		FROM pg_tables 
		WHERE schemaname = 'public'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, seqName string
		var seqValue sql.NullInt64

		if err := rows.Scan(&tableName, &seqName); err != nil {
			return nil, err
		}

		if seqName != "" {
			err := connection.Get(&seqValue, fmt.Sprintf("SELECT last_value FROM %s", seqName))
			if err == nil && seqValue.Valid {
				metadata[tableName] = seqValue.Int64
			}
		}
	}

	return metadata, nil
}

// RestoreMetadata sets sequence/identity values for tables
func (connection *DbConnection) RestoreMetadata(s map[string]any) error {
	for tableName, v := range s {
		id, ok := v.(float64)
		if !ok {
			log.Error().Str("table", tableName).Msg("failed to restore metadata")
			continue
		}

		seqName := fmt.Sprintf("%s_id_seq", tableName)
		_, err := connection.Exec(fmt.Sprintf("ALTER SEQUENCE %s RESTART WITH %d", seqName, int64(id)))
		if err != nil {
			log.Error().Err(err).Str("table", tableName).Msg("failed to restore sequence")
		}
	}

	return nil
}