package database

import (
	"errors"
	"fmt"

	portainer "github.com/portainer/portainer/api"
	"github.com/portainer/portainer/api/database/boltdb"
	"github.com/portainer/portainer/api/database/postgres"
)
var (
	ErrUnknownStoreType = errors.New("unknown database store type")
	ErrEmptyStorePath   = errors.New("store path cannot be empty")
	ErrConnectionFailed = errors.New("failed to establish database connection")
)
// NewDatabase should use config options to return a connection to the requested database
func NewDatabase(storeType, storePath string, encryptionKey []byte) (connection portainer.Connection, err error) {
	switch storeType {
	case "boltdb":
		return &boltdb.DbConnection{
			Path:          storePath,
			EncryptionKey: encryptionKey,
		}, nil
	case "postgres":
		_, err := postgres.NewConnection("postgres://sudip:sudip152@localhost:5432/portainer?sslmode=disable", encryptionKey)
		
		return connection, err
	default:
		return nil, fmt.Errorf("unknown storage database: %s", storeType)
	}
}