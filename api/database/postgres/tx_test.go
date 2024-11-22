// package postgres

// import (
// 	"errors"
// 	"os"
// 	"testing"
// 	"time"

// 	portainer "github.com/portainer/portainer/api"
// 	"github.com/portainer/portainer/api/dataservices"
// 	"github.com/stretchr/testify/assert"
// )

// const testTableName = "test_table"
// const testId = 1234

// type testStruct struct {
// 	Key   string
// 	Value string
// }

// func setupTestDB(t *testing.T) *DbConnection {
// 	// Use environment variables or test configuration
// 	connStr := os.Getenv("TEST_DATABASE_URL")
// 	if connStr == "" {
// 		connStr = "postgres://postgres:postgres@localhost:5432/portainer_test?sslmode=disable"
// 	}

// 	conn := &DbConnection{
// 		ConnectionURL: connStr,
// 	}

// 	err := conn.Open()
// 	if err != nil {
// 		t.Fatalf("Failed to open database connection: %v", err)
// 	}

// 	// Create test table
// 	createTableSQL := `
// 		CREATE TABLE IF NOT EXISTS test_table (
// 			id INTEGER PRIMARY KEY,
// 			data JSONB
// 		)`

// 	_, err = conn.db.Exec(createTableSQL)
// 	if err != nil {
// 		t.Fatalf("Failed to create test table: %v", err)
// 	}

// 	return conn
// }

// func cleanupTestDB(t *testing.T, conn *DbConnection) {
// 	_, err := conn.db.Exec("DROP TABLE IF EXISTS test_table")
// 	if err != nil {
// 		t.Errorf("Failed to cleanup test table: %v", err)
// 	}
// 	conn.Close()
// }

// func TestTxs(t *testing.T) {
// 	conn := setupTestDB(t)
// 	defer cleanupTestDB(t, conn)

// 	// Error propagation
// 	err := conn.UpdateTx(func(tx portainer.Transaction) error {
// 		return errors.New("this is an error")
// 	})
// 	assert.Error(t, err, "an error was expected")

// 	// Create an object
// 	newObj := testStruct{
// 		Key:   "key",
// 		Value: "value",
// 	}

// 	err = conn.UpdateTx(func(tx portainer.Transaction) error {
// 		err = tx.SetServiceName(testTableName)
// 		if err != nil {
// 			return err
// 		}

// 		return tx.CreateObjectWithId(testTableName, testId, newObj)
// 	})
// 	assert.NoError(t, err)

// 	// Read the object
// 	obj := testStruct{}
// 	err = conn.ViewTx(func(tx portainer.Transaction) error {
// 		return tx.GetObject(testTableName, conn.ConvertToKey(testId), &obj)
// 	})
// 	assert.NoError(t, err)
// 	assert.Equal(t, newObj.Key, obj.Key)
// 	assert.Equal(t, newObj.Value, obj.Value)

// 	// Update an object
// 	updatedObj := testStruct{
// 		Key:   "updated-key",
// 		Value: "updated-value",
// 	}

// 	err = conn.UpdateTx(func(tx portainer.Transaction) error {
// 		return tx.UpdateObject(testTableName, conn.ConvertToKey(testId), &updatedObj)
// 	})
// 	assert.NoError(t, err)

// 	err = conn.ViewTx(func(tx portainer.Transaction) error {
// 		return tx.GetObject(testTableName, conn.ConvertToKey(testId), &obj)
// 	})
// 	assert.NoError(t, err)
// 	assert.Equal(t, updatedObj.Key, obj.Key)
// 	assert.Equal(t, updatedObj.Value, obj.Value)

// 	// Delete an object
// 	err = conn.UpdateTx(func(tx portainer.Transaction) error {
// 		return tx.DeleteObject(testTableName, conn.ConvertToKey(testId))
// 	})
// 	assert.NoError(t, err)

// 	err = conn.ViewTx(func(tx portainer.Transaction) error {
// 		return tx.GetObject(testTableName, conn.ConvertToKey(testId), &obj)
// 	})
// 	assert.True(t, dataservices.IsErrObjectNotFound(err))

// 	// Test GetNextIdentifier
// 	err = conn.UpdateTx(func(tx portainer.Transaction) error {
// 		id1 := tx.GetNextIdentifier(testTableName)
// 		id2 := tx.GetNextIdentifier(testTableName)

// 		if id1+1 != id2 {
// 			return errors.New("unexpected identifier sequence")
// 		}

// 		return nil
// 	})
// 	assert.NoError(t, err)

// 	// Test write operation in read-only transaction
// 	err = conn.ViewTx(func(tx portainer.Transaction) error {
// 		return tx.CreateObjectWithId(testTableName, testId, newObj)
// 	})
// 	assert.Error(t, err, "an error was expected for write operation in read-only transaction")
// }

// // Additional PostgreSQL-specific transaction tests
// func TestTransactionIsolation(t *testing.T) {
// 	conn := setupTestDB(t)
// 	defer cleanupTestDB(t, conn)

// 	// Test concurrent transactions
// 	errChan := make(chan error, 2)

// 	// Start two concurrent transactions
// 	go func() {
// 		err := conn.UpdateTx(func(tx portainer.Transaction) error {
// 			// Simulate long running transaction
// 			time.Sleep(100 * time.Millisecond)
// 			return tx.CreateObjectWithId(testTableName, 1, testStruct{Key: "tx1", Value: "value1"})
// 		})
// 		errChan <- err
// 	}()

// 	go func() {
// 		err := conn.UpdateTx(func(tx portainer.Transaction) error {
// 			return tx.CreateObjectWithId(testTableName, 2, testStruct{Key: "tx2", Value: "value2"})
// 		})
// 		errChan <- err
// 	}()

// 	// Wait for both transactions
// 	for i := 0; i < 2; i++ {
// 		err := <-errChan
// 		assert.NoError(t, err)
// 	}

// 	// Verify both objects exist
// 	var obj1, obj2 testStruct
// 	err := conn.ViewTx(func(tx portainer.Transaction) error {
// 		err1 := tx.GetObject(testTableName, conn.ConvertToKey(1), &obj1)
// 		err2 := tx.GetObject(testTableName, conn.ConvertToKey(2), &obj2)
// 		return errors.Join(err1, err2)
// 	})

// 	assert.NoError(t, err)
// 	assert.Equal(t, "tx1", obj1.Key)
// 	assert.Equal(t, "tx2", obj2.Key)
// }

// func TestTransactionRollback(t *testing.T) {
// 	conn := setupTestDB(t)
// 	defer cleanupTestDB(t, conn)

// 	// Create initial object
// 	initialObj := testStruct{Key: "initial", Value: "value"}
// 	err := conn.UpdateTx(func(tx portainer.Transaction) error {
// 		return tx.CreateObjectWithId(testTableName, testId, initialObj)
// 	})
// 	assert.NoError(t, err)

// 	// Attempt update with rollback
// 	err = conn.UpdateTx(func(tx portainer.Transaction) error {
// 		err := tx.UpdateObject(testTableName, conn.ConvertToKey(testId), &testStruct{Key: "updated", Value: "value"})
// 		if err != nil {
// 			return err
// 		}
// 		return errors.New("force rollback")
// 	})
// 	assert.Error(t, err)

// 	// Verify object remains unchanged
// 	var obj testStruct
// 	err = conn.ViewTx(func(tx portainer.Transaction) error {
// 		return tx.GetObject(testTableName, conn.ConvertToKey(testId), &obj)
// 	})
// 	assert.NoError(t, err)
// 	assert.Equal(t, initialObj.Key, obj.Key)
// }