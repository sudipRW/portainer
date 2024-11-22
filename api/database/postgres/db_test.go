package postgres

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func Test_NeedsEncryptionMigration(t *testing.T) {
	is := assert.New(t)

	// Mock database setup
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create sqlmock: %v", err)
	}
	defer db.Close()

	cases := []struct {
		name         string
		dbname       string
		key          bool
		expectError  error
		expectResult bool
		setupMocks   func()
	}{
		{
			name:         "portainer.edb + key",
			dbname:       EncryptedDatabaseName,
			key:          true,
			expectError:  nil,
			expectResult: false,
			setupMocks: func() {
				mock.ExpectQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'encrypted_metadata'").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			},
		},
		{
			name:         "portainer.db + key (migration needed)",
			dbname:       UnencryptedDatabaseName,
			key:          true,
			expectError:  nil,
			expectResult: true,
			setupMocks: func() {
				mock.ExpectQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'unencrypted_metadata'").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			},
		},
		{
			name:         "portainer.db + no key",
			dbname:       UnencryptedDatabaseName,
			key:          false,
			expectError:  nil,
			expectResult: false,
			setupMocks: func() {
				mock.ExpectQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'unencrypted_metadata'").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			},
		},
		{
			name:         "NoDB (new) + key",
			dbname:       "",
			key:          true,
			expectError:  nil,
			expectResult: false,
			setupMocks: func() {
				mock.ExpectQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'unencrypted_metadata'").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			},
		},
		{
			name:         "NoDB (new) + no key",
			dbname:       "",
			key:          false,
			expectError:  nil,
			expectResult: false,
			setupMocks: func() {
				mock.ExpectQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'unencrypted_metadata'").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			},
		},
		{
			name:         "portainer.edb + no key",
			dbname:       EncryptedDatabaseName,
			key:          false,
			expectError:  ErrHaveEncryptedWithNoKey,
			expectResult: false,
			setupMocks: func() {
				mock.ExpectQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'encrypted_metadata'").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			},
		},
		{
			name:         "portainer.db & portainer.edb",
			dbname:       "both",
			key:          true,
			expectError:  ErrHaveEncryptedAndUnencrypted,
			expectResult: false,
			setupMocks: func() {
				mock.ExpectQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'encrypted_metadata'").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
				mock.ExpectQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'unencrypted_metadata'").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			connection := DbConnection{DB: db}

			if tc.key {
				connection.EncryptionKey = []byte("secret")
			}

			if tc.setupMocks != nil {
				tc.setupMocks()
			}

			result, err := connection.NeedsEncryptionMigration()

			is.Equal(tc.expectError, err, "Fatal Error failure. Test: %s", tc.name)
			is.Equal(result, tc.expectResult, "Failed test: %s", tc.name)

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}
