package sync

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type testTable struct {
	name    string
	schema  string
	srcData [][]interface{}
	tgtData [][]interface{}
}

func TestSync(t *testing.T) {
	// Create temp directory for test databases
	tmpDir, err := os.MkdirTemp("", "sync_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	srcPath := filepath.Join(tmpDir, "test_src.db")
	tgtPath := filepath.Join(tmpDir, "test_tgt.db")

	// Set command line arguments for the sync function
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"cmd", srcPath, tgtPath}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	tests := []struct {
		name      string
		tables    []testTable
		config    Config
		expected  map[string][][]interface{}
		wantError bool
	}{
		{
			name: "Multiple tables sync",
			tables: []testTable{
				{
					name: "users",
					schema: `CREATE TABLE users (
						id INTEGER PRIMARY KEY,
						name TEXT NOT NULL,
						email TEXT
					)`,
					srcData: [][]interface{}{
						{1, "Alice", "alice@test.com"},
						{2, "Bob", "bob@test.com"},
					},
					tgtData: [][]interface{}{
						{1, "Alice Old", "alice.old@test.com"},
						{3, "Charlie", "charlie@test.com"},
					},
				},
				{
					name: "orders",
					schema: `CREATE TABLE orders (
						order_id INTEGER PRIMARY KEY,
						user_id INTEGER,
						amount REAL,
						FOREIGN KEY(user_id) REFERENCES users(id)
					)`,
					srcData: [][]interface{}{
						{101, 1, 99.99},
						{102, 2, 149.99},
					},
					tgtData: [][]interface{}{
						{101, 1, 89.99},
						{103, 3, 199.99},
					},
				},
			},
			config: Config{},
			expected: map[string][][]interface{}{
				"users": {
					{1, "Alice", "alice@test.com"},
					{2, "Bob", "bob@test.com"},
				},
				"orders": {
					{101, 1, 99.99},
					{102, 2, 149.99},
				},
			},
		},
		{
			name: "Filter greater than sync",
			tables: []testTable{
				{
					name: "products",
					schema: `CREATE TABLE products (
						id INTEGER PRIMARY KEY,
						name TEXT,
						price REAL
					)`,
					srcData: [][]interface{}{
						{1, "Item 1", 10.0},
						{2, "Item 2", 20.0},
						{3, "Item 3", 30.0},
					},
					tgtData: [][]interface{}{
						{1, "Old Item 1", 9.0},
						{2, "Old Item 2", 19.0},
						{3, "Old Item 3", 29.0},
					},
				},
			},
			config: Config{
				Filter: "gt",
				Value:  "1",
			},
			expected: map[string][][]interface{}{
				"products": {
					{1, "Old Item 1", 9.0},
					{2, "Item 2", 20.0},
					{3, "Item 3", 30.0},
				},
			},
		},
		{
			name: "Filter less than sync",
			tables: []testTable{
				{
					name: "products",
					schema: `CREATE TABLE products (
						id INTEGER PRIMARY KEY,
						name TEXT,
						price REAL
					)`,
					srcData: [][]interface{}{
						{1, "Item 1", 10.0},
						{2, "Item 2", 20.0},
						{3, "Item 3", 30.0},
					},
					tgtData: [][]interface{}{
						{1, "Old Item 1", 9.0},
						{2, "Old Item 2", 19.0},
						{3, "Old Item 3", 29.0},
					},
				},
			},
			config: Config{
				Filter: "lt",
				Value:  "3",
			},
			expected: map[string][][]interface{}{
				"products": {
					{1, "Item 1", 10.0},
					{2, "Item 2", 20.0},
					{3, "Old Item 3", 29.0},
				},
			},
		},
		{
			name: "Filter greater than or equal sync",
			tables: []testTable{
				{
					name: "products",
					schema: `CREATE TABLE products (
						id INTEGER PRIMARY KEY,
						name TEXT,
						price REAL
					)`,
					srcData: [][]interface{}{
						{1, "Item 1", 10.0},
						{2, "Item 2", 20.0},
						{3, "Item 3", 30.0},
					},
					tgtData: [][]interface{}{
						{1, "Old Item 1", 9.0},
						{2, "Old Item 2", 19.0},
						{3, "Old Item 3", 29.0},
					},
				},
			},
			config: Config{
				Filter: "gte",
				Value:  "2",
			},
			expected: map[string][][]interface{}{
				"products": {
					{1, "Old Item 1", 9.0},
					{2, "Item 2", 20.0},
					{3, "Item 3", 30.0},
				},
			},
		},
		{
			name: "Filter less than or equal sync",
			tables: []testTable{
				{
					name: "products",
					schema: `CREATE TABLE products (
						id INTEGER PRIMARY KEY,
						name TEXT,
						price REAL
					)`,
					srcData: [][]interface{}{
						{1, "Item 1", 10.0},
						{2, "Item 2", 20.0},
						{3, "Item 3", 30.0},
					},
					tgtData: [][]interface{}{
						{1, "Old Item 1", 9.0},
						{2, "Old Item 2", 19.0},
						{3, "Old Item 3", 29.0},
					},
				},
			},
			config: Config{
				Filter: "lte",
				Value:  "2",
			},
			expected: map[string][][]interface{}{
				"products": {
					{1, "Item 1", 10.0},
					{2, "Item 2", 20.0},
					{3, "Old Item 3", 29.0},
				},
			},
		},
		{
			name: "No-delete option",
			tables: []testTable{
				{
					name: "categories",
					schema: `CREATE TABLE categories (
						id INTEGER PRIMARY KEY,
						name TEXT
					)`,
					srcData: [][]interface{}{
						{1, "Category 1"},
						{2, "Category 2"},
					},
					tgtData: [][]interface{}{
						{1, "Old Category 1"},
						{3, "Category 3"},
					},
				},
			},
			config: Config{
				NoDelete: true,
			},
			expected: map[string][][]interface{}{
				"categories": {
					{1, "Category 1"},
					{2, "Category 2"},
					{3, "Category 3"},
				},
			},
		},
		{
			name: "Filter with no-delete option",
			tables: []testTable{
				{
					name: "products",
					schema: `CREATE TABLE products (
						id INTEGER PRIMARY KEY,
						name TEXT,
						price REAL
					)`,
					srcData: [][]interface{}{
						{1, "Item 1", 10.0},
						{2, "Item 2", 20.0},
						{3, "Item 3", 30.0},
					},
					tgtData: [][]interface{}{
						{1, "Old Item 1", 9.0},
						{2, "Old Item 2", 19.0},
						{3, "Old Item 3", 29.0},
						{4, "Old Item 4", 39.0},
					},
				},
			},
			config: Config{
				Filter:   "gte",
				Value:    "2",
				NoDelete: true,
			},
			expected: map[string][][]interface{}{
				"products": {
					{1, "Old Item 1", 9.0},
					{2, "Item 2", 20.0},
					{3, "Item 3", 30.0},
					{4, "Old Item 4", 39.0},
				},
			},
		},
		{
			name: "Sync specific tables only",
			tables: []testTable{
				{
					name: "users",
					schema: `CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT
            )`,
					srcData: [][]interface{}{
						{1, "Alice", "alice@test.com"},
						{2, "Bob", "bob@test.com"},
					},
					tgtData: [][]interface{}{
						{1, "Alice Old", "alice.old@test.com"},
						{3, "Charlie", "charlie@test.com"},
					},
				},
				{
					name: "orders",
					schema: `CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                user_id INTEGER,
                amount REAL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )`,
					srcData: [][]interface{}{
						{101, 1, 99.99},
						{102, 2, 149.99},
					},
					tgtData: [][]interface{}{
						{101, 1, 89.99},
						{103, 3, 199.99},
					},
				},
			},
			config: Config{
				Tables: []string{"users"}, // Only sync the users table
			},
			expected: map[string][][]interface{}{
				"users": {
					{1, "Alice", "alice@test.com"},
					{2, "Bob", "bob@test.com"},
				},
				"orders": { // Orders table should remain unchanged
					{101, 1, 89.99},
					{103, 3, 199.99},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fresh databases for each test
			err := os.RemoveAll(tmpDir)
			if err != nil {
				t.Fatalf("Failed to clean temp dir: %v", err)
			}
			err = os.MkdirAll(tmpDir, 0755)
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}

			srcDB, err := createTestDB(srcPath, tt.tables)
			if err != nil {
				t.Fatalf("Failed to create source database: %v", err)
			}
			defer srcDB.Close()

			tgtDB, err := createTestDB(tgtPath, tt.tables)
			if err != nil {
				t.Fatalf("Failed to create target database: %v", err)
			}
			defer tgtDB.Close()

			// Setup initial data
			for _, table := range tt.tables {

				if err := insertTestData(srcDB, table.name, table.srcData); err != nil {
					t.Fatalf("Failed to insert source data for table %s: %v", table.name, err)
				}
				if err := insertTestData(tgtDB, table.name, table.tgtData); err != nil {
					t.Fatalf("Failed to insert target data for table %s: %v", table.name, err)
				}

				// Verify initial data
				srcData, err := getTableData(srcDB, table.name)
				if err != nil {
					t.Fatalf("Failed to verify source data for table %s: %v", table.name, err)
				}
				if !compareData(srcData, table.srcData) {
					t.Fatalf("Initial source data mismatch for table %s", table.name)
				}

				tgtData, err := getTableData(tgtDB, table.name)
				if err != nil {
					t.Fatalf("Failed to verify target data for table %s: %v", table.name, err)
				}
				if !compareData(tgtData, table.tgtData) {
					t.Fatalf("Initial target data mismatch for table %s", table.name)
				}
			}

			// Set up the config
			config := Config{
				Filter:    tt.config.Filter,
				Value:     tt.config.Value,
				NoDelete:  tt.config.NoDelete,
				Tables:    tt.config.Tables,
				SrcDbPath: srcPath,
				DstDbPath: tgtPath,
			}

			// Perform sync
			err = Sync(config)
			if (err != nil) != tt.wantError {
				t.Fatalf("Sync() error = %v, wantError %v, config: %+v", err, tt.wantError, config)
			}

			// Verify results
			for tableName, expectedData := range tt.expected {
				gotData, err := getTableData(tgtDB, tableName)
				if err != nil {
					t.Fatalf("Failed to get table data: %v", err)
				}

				if !compareData(gotData, expectedData) {
					// Print detailed data comparison
					t.Errorf("Table %s data mismatch with config %+v", tableName, tt.config)
					t.Errorf("Got data:")
					for _, row := range gotData {
						t.Errorf("  %v", row)
					}
					t.Errorf("Want data:")
					for _, row := range expectedData {
						t.Errorf("  %v", row)
					}
				}
			}

			// Clean up test databases
			srcDB.Close()
			tgtDB.Close()
		})
	}
}

func createTestDB(path string, tables []testTable) (*sql.DB, error) {
	// Remove existing database file if it exists
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("removing existing database: %w", err)
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Create tables
	for _, table := range tables {
		if _, err := db.Exec(table.schema); err != nil {
			db.Close()
			return nil, fmt.Errorf("creating table %s: %w", table.name, err)
		}
	}

	return db, nil
}

func insertTestData(db *sql.DB, tableName string, data [][]interface{}) error {
	if len(data) == 0 {
		return nil
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	// Create prepared statement
	placeholders := make([]string, len(data[0]))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (%s)",
		tableName, joinStrings(placeholders, ",")))
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	// Insert data
	for _, row := range data {
		if _, err := stmt.Exec(row...); err != nil {
			return fmt.Errorf("inserting row %v: %w", row, err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

func getTableData(db *sql.DB, tableName string) ([][]interface{}, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY rowid", tableName))
	if err != nil {
		return nil, fmt.Errorf("querying table %s: %w", tableName, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting columns: %w", err)
	}

	var result [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		scanArgs := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		row := make([]interface{}, len(cols))
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = v
			}
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return result, nil
}

func compareData(got, want [][]interface{}) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if len(got[i]) != len(want[i]) {
			return false
		}
		for j := range got[i] {
			g := normalize(got[i][j])
			w := normalize(want[i][j])
			if g != w {
				return false
			}
		}
	}
	return true
}

func normalize(v interface{}) interface{} {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case int64:
		return int(x)
	case float64:
		return float32(x)
	default:
		return v
	}
}

func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for _, s := range strs[1:] {
		result += sep + s
	}
	return result
}
