package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx"
	dserrors "github.com/portainer/portainer/api/dataservices/errors"

	"github.com/rs/zerolog/log"
)

type DbTransaction struct {
	conn *DbConnection
	tx   *sqlx.Tx
}

func (tx *DbTransaction) SetServiceName(bucketName string) error {
	// In PostgreSQL, this would typically involve creating a table if it doesn't exist
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			data JSONB NOT NULL
		)`, bucketName)
	_, err := tx.tx.Exec(createTableQuery)
	return err
}

func (tx *DbTransaction) GetObject(bucketName string, key []byte, object any) error {
	query := fmt.Sprintf("SELECT data FROM %s WHERE id = $1", bucketName)
	
	var jsonData []byte
	err := tx.tx.Get(&jsonData, query, string(key))
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w (bucket=%s, key=%s)", dserrors.ErrObjectNotFound, bucketName, string(key))
	} else if err != nil {
		return err
	}

	return json.Unmarshal(jsonData, object)
}

func (tx *DbTransaction) UpdateObject(bucketName string, key []byte, object any) error {
	data, err := json.Marshal(object)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("UPDATE %s SET data = $1 WHERE id = $2", bucketName)
	_, err = tx.tx.Exec(query, data, string(key))
	return err
}

func (tx *DbTransaction) DeleteObject(bucketName string, key []byte) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", bucketName)
	_, err := tx.tx.Exec(query, string(key))
	return err
}

func (tx *DbTransaction) DeleteAllObjects(bucketName string, obj any, matchingFn func(o any) (id int, ok bool)) error {
	// Retrieve all objects
	query := fmt.Sprintf("SELECT id, data FROM %s", bucketName)
	rows, err := tx.tx.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var idsToDelete []int

	// Use reflection to create a new slice of the same type as obj
	objType := reflect.TypeOf(obj)
	// objSliceType := reflect.SliceOf(objType)
	// objSlice := reflect.New(objSliceType).Elem()

	for rows.Next() {
		var id string
		var jsonData []byte

		if err := rows.Scan(&id, &jsonData); err != nil {
			return err
		}

		// Unmarshal the object
		tempObj := reflect.New(objType).Elem()
		if err := json.Unmarshal(jsonData, tempObj.Addr().Interface()); err != nil {
			return err
		}

		// Check if the object matches the deletion criteria
		if deleteID, ok := matchingFn(tempObj.Interface()); ok {
			idsToDelete = append(idsToDelete, deleteID)
		}
	}

	// Delete matching objects
	for _, id := range idsToDelete {
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE id = $1", bucketName)
		_, err := tx.tx.Exec(deleteQuery, id)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tx *DbTransaction) GetNextIdentifier(bucketName string) int {
	var nextID int
	query := fmt.Sprintf("SELECT COALESCE(MAX(id), 0) + 1 FROM %s", bucketName)
	err := tx.tx.Get(&nextID, query)
	if err != nil {
		log.Error().Err(err).Str("bucket", bucketName).Msg("failed to get the next identifier")
		return 0
	}
	return nextID
}

func (tx *DbTransaction) CreateObject(bucketName string, fn func(uint64) (int, any)) error {
	// Get the next sequence number
	var seqID uint64
	query := fmt.Sprintf("SELECT COALESCE(MAX(id), 0) + 1 FROM %s", bucketName)
	err := tx.tx.Get(&seqID, query)
	if err != nil {
		return err
	}

	// Generate the object
	id, obj := fn(seqID)

	// Marshall the object
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	// Insert the object
	insertQuery := fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", bucketName)
	_, err = tx.tx.Exec(insertQuery, id, data)
	return err
}

func (tx *DbTransaction) CreateObjectWithId(bucketName string, id int, obj any) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", bucketName)
	_, err = tx.tx.Exec(query, id, data)
	return err
}

func (tx *DbTransaction) CreateObjectWithStringId(bucketName string, id []byte, obj any) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", bucketName)
	_, err = tx.tx.Exec(query, string(id), data)
	return err
}

func (tx *DbTransaction) GetAll(bucketName string, obj any, appendFn func(o any) (any, error)) error {
	query := fmt.Sprintf("SELECT data FROM %s", bucketName)
	rows, err := tx.tx.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var jsonData []byte
		if err := rows.Scan(&jsonData); err != nil {
			return err
		}

		// Unmarshal the object
		err := json.Unmarshal(jsonData, obj)
		if err != nil {
			return err
		}

		// Call the append function
		obj, err = appendFn(obj)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tx *DbTransaction) GetAllWithKeyPrefix(bucketName string, keyPrefix []byte, obj any, appendFn func(o any) (any, error)) error {
	query := fmt.Sprintf("SELECT data FROM %s WHERE id LIKE $1", bucketName)
	rows, err := tx.tx.Query(query, string(keyPrefix)+"%")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var jsonData []byte
		if err := rows.Scan(&jsonData); err != nil {
			return err
		}

		// Unmarshal the object
		err := json.Unmarshal(jsonData, obj)
		if err != nil {
			return err
		}

		// Call the append function
		obj, err = appendFn(obj)
		if err != nil {
			return err
		}
	}

	return nil
}