package postgres

import (
	"database/sql"
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
	DatabaseDriverName = "postgres"
	DatabaseMaxOpen    = 25
	DatabaseMaxIdle    = 25
)

const (
    EncryptedMetadataTable   = "encrypted_metadata"
    UnencryptedMetadataTable = "unencrypted_metadata"
)


var (
	ErrHaveEncryptedAndUnencrypted = errors.New("Portainer has detected both an encrypted and un-encrypted database and cannot start")
	ErrHaveEncryptedWithNoKey      = errors.New("The portainer database is encrypted, but no secret was loaded")
)

type DbConnection struct {
	ConnectionString string
	Path            string
	EncryptionKey   []byte
	isEncrypted     bool

	*sqlx.DB
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

// NeedsEncryptionMigration checks the database state to determine if migration is required.
func (connection *DbConnection) NeedsEncryptionMigration() (bool, error) {
	// Query to check for the existence of specific tables
	checkTableExists := func(tableName string) (bool, error) {
		var count int
		query := fmt.Sprintf(`
			SELECT COUNT(*) 
			FROM information_schema.tables 
			WHERE table_name = '%s';`, tableName)
		err := connection.DB.QueryRow(query).Scan(&count)
		if err != nil {
			return false, err
		}
		return count > 0, nil
	}

	// Check for unencrypted table (portainer.db equivalent)
	haveUnencryptedTable, err := checkTableExists(UnencryptedMetadataTable)
	if err != nil {
		return false, err
	}

	// Check for encrypted table (portainer.edb equivalent)
	haveEncryptedTable, err := checkTableExists(EncryptedMetadataTable)
	if err != nil {
		return false, err
	}

	if haveUnencryptedTable && haveEncryptedTable {
		// Case 7: both encrypted and unencrypted tables exist
		return false, ErrHaveEncryptedAndUnencrypted
	}

	if haveUnencryptedTable && connection.EncryptionKey != nil {
		// Case 3: unencrypted table exists, and an encryption key is provided
		return true, nil // Needs migration
	}

	if haveEncryptedTable && connection.EncryptionKey == nil {
		// Case 2: encrypted table exists, but no encryption key is provided
		return false, ErrHaveEncryptedWithNoKey
	}

	// Case 1, 4, 5, 6: no conflicting conditions
	return false, nil
}

// Open opens and initializes the PostgreSQL database connection
func (connection *DbConnection) Open() error {
	log.Info().Str("connection", connection.ConnectionString).Msg("loading PortainerDB")

	db, err := sqlx.Open(DatabaseDriverName, connection.ConnectionString)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(DatabaseMaxOpen)
	db.SetMaxIdleConns(DatabaseMaxIdle)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test the connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	connection.DB = db
	return nil
}

// Close closes the PostgreSQL database connection
func (connection *DbConnection) Close() error {
	log.Info().Msg("closing PortainerDB")

	if connection.DB != nil {
		return connection.DB.Close()
	}

	return nil
}

// UpdateTx executes the given function inside a transaction
func (connection *DbConnection) UpdateTx(fn func(portainer.Transaction) error) error {
	tx, err := connection.Beginx()
	if err != nil {
		return err
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
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// ViewTx executes a read-only transaction (in PostgreSQL, this is similar to UpdateTx)
func (connection *DbConnection) ViewTx(fn func(portainer.Transaction) error) error {
	return connection.UpdateTx(fn)
}

// BackupTo exports the database to a writer (simplified for PostgreSQL)
func (connection *DbConnection) BackupTo(w io.Writer) error {
	// In PostgreSQL, you might want to use pg_dump or a more robust backup method
	rows, err := connection.DB.Query("SELECT schemaname, tablename, tableowner FROM pg_tables WHERE schemaname='public'")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, tableName, tableOwner string
		if err := rows.Scan(&schemaName, &tableName, &tableOwner); err != nil {
			return err
		}
		// Write table info to writer
		fmt.Fprintf(w, "Table: %s, Schema: %s, Owner: %s\n", tableName, schemaName, tableOwner)
	}

	return nil
}

// GetNextIdentifier retrieves the next identifier for a given bucket/table
func (connection *DbConnection) GetNextIdentifier(bucketName string) int {
	var nextID int
	query := fmt.Sprintf("SELECT COALESCE(MAX(id), 0) + 1 FROM %s", bucketName)
	err := connection.Get(&nextID, query)
	if err != nil {
		log.Error().Err(err).Str("bucket", bucketName).Msg("failed to get next identifier")
		return 0
	}
	return nextID
}

// CreateObject creates a new object in the specified table
func (connection *DbConnection) CreateObject(bucketName string, fn func(uint64) (int, any)) error {
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

func (connection *DbConnection) getEncryptionKey() []byte {
	if !connection.isEncrypted {
		return nil
	}
	return connection.EncryptionKey
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