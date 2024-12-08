package sync

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// Modify the existing Config struct to add arg tags
type Config struct {
	Filter    string   `arg:"-f" help:"filter type: gt, lt, gte, or lte"`
	Value     string   `arg:"-v" help:"filter value"`
	NoDelete  bool     `arg:"-n,--nodelete" help:"don't delete records from target"`
	Tables    []string `arg:"-t,--tables,separate" help:"tables to sync (if not specified, syncs all tables)"`
	SrcDbPath string   `arg:"positional,required" help:"source database path"`
	DstDbPath string   `arg:"positional,required" help:"target database path"`
}

func (Config) Description() string {
	return "Syncs data between two SQLite databases with filtering options"
}

func Sync(cfg Config) error {
	src, err := sql.Open("sqlite3", cfg.SrcDbPath)
	if err != nil {
		return fmt.Errorf("opening source db: %w", err)
	}
	defer src.Close()

	dst, err := sql.Open("sqlite3", cfg.DstDbPath)
	if err != nil {
		return fmt.Errorf("opening target db: %w", err)
	}
	defer dst.Close()

	tables, err := getTables(src)
	if err != nil {
		return err
	}

	// Add this block to filter tables if specified
	if len(cfg.Tables) > 0 {
		tableMap := make(map[string]bool)
		for _, t := range cfg.Tables {
			tableMap[t] = true
		}

		filteredTables := make([]Table, 0)
		for _, table := range tables {
			if tableMap[table.name] {
				filteredTables = append(filteredTables, table)
			}
		}
		tables = filteredTables
	}

	for _, table := range tables {
		if err := syncTable(src, dst, table, cfg); err != nil {
			return fmt.Errorf("syncing table %s: %w", table.name, err)
		}
	}
	return nil
}

type Table struct {
	name    string
	columns []string
	pkCol   string
}

func getTables(db *sql.DB) ([]Table, error) {
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []Table
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		table, err := getTableInfo(db, name)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func getTableInfo(db *sql.DB, tableName string) (Table, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return Table{}, err
	}
	defer rows.Close()

	var table Table
	table.name = tableName

	for rows.Next() {
		var (
			cid      int
			name     string
			type_    string
			notnull  int
			dflt_val sql.NullString
			pk       int
		)
		if err := rows.Scan(&cid, &name, &type_, &notnull, &dflt_val, &pk); err != nil {
			return Table{}, err
		}
		table.columns = append(table.columns, name)
		if pk > 0 {
			table.pkCol = name
		}
	}

	if table.pkCol == "" {
		table.pkCol = "rowid" // SQLite default
	}

	return table, nil
}

func syncTable(src, dst *sql.DB, table Table, cfg Config) error {
	tx, err := dst.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Prepare statements
	insertQuery := buildInsertQuery(table)
	insert, err := tx.Prepare(insertQuery)
	if err != nil {
		return err
	}
	defer insert.Close()

	deleteStmt, err := tx.Prepare(fmt.Sprintf("DELETE FROM %s WHERE %s = ?", table.name, table.pkCol))
	if err != nil {
		return err
	}
	defer deleteStmt.Close()

	// Sync rows from source to target
	selectQuery := buildSelectQuery(table, cfg)
	var rows *sql.Rows
	if cfg.Value != "" {
		rows, err = src.Query(selectQuery, cfg.Value)
	} else {
		rows, err = src.Query(selectQuery)
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	cols := append([]string{table.pkCol}, table.columns...)
	values := make([]interface{}, len(cols))
	scanPtrs := make([]interface{}, len(cols))
	for i := range values {
		scanPtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			return err
		}
		if _, err := insert.Exec(values...); err != nil {
			return err
		}
	}

	// Delete orphaned rows if not using no-delete flag
	if !cfg.NoDelete {
		// Get list of IDs from source
		var sourceIDs []interface{}
		srcRows, err := src.Query(fmt.Sprintf("SELECT %s FROM %s", table.pkCol, table.name))
		if err != nil {
			return fmt.Errorf("querying source IDs: %w", err)
		}
		defer srcRows.Close()

		for srcRows.Next() {
			var id interface{}
			if err := srcRows.Scan(&id); err != nil {
				return fmt.Errorf("scanning source ID: %w", err)
			}
			sourceIDs = append(sourceIDs, id)
		}

		// Delete rows from target that don't exist in source
		if len(sourceIDs) > 0 {
			placeholders := strings.Repeat("?,", len(sourceIDs))
			placeholders = placeholders[:len(placeholders)-1] // Remove trailing comma
			query := fmt.Sprintf("DELETE FROM %s WHERE %s NOT IN (%s)",
				table.name, table.pkCol, placeholders)

			if _, err := tx.Exec(query, sourceIDs...); err != nil {
				return fmt.Errorf("deleting orphaned rows: %w", err)
			}
		}
	}

	return tx.Commit()
}

func buildSelectQuery(table Table, cfg Config) string {
	cols := append([]string{table.pkCol}, table.columns...)
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ", "), table.name)

	if cfg.Filter != "" && cfg.Value != "" {
		var op string
		switch cfg.Filter {
		case "gt":
			op = ">"
		case "lt":
			op = "<"
		case "gte":
			op = ">="
		case "lte":
			op = "<="
		}
		if op != "" {
			query += fmt.Sprintf(" WHERE %s %s ?", table.pkCol, op)
		}
	}
	return query
}

func buildInsertQuery(table Table) string {
	cols := append([]string{table.pkCol}, table.columns...)
	placeholders := make([]string, len(cols))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return fmt.Sprintf(
		"INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		table.name,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)
}

func deleteOrphans(src, dst *sql.DB, table Table, deleteStmt *sql.Stmt, cfg Config) error {
	query := buildSelectQuery(table, cfg)
	var rows *sql.Rows
	var err error

	if cfg.Value != "" {
		rows, err = dst.Query(query, cfg.Value)
	} else {
		rows, err = dst.Query(query)
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	values := make([]interface{}, len(table.columns)+1)
	scanPtrs := make([]interface{}, len(values))
	for i := range values {
		scanPtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			return err
		}

		var exists bool
		err := src.QueryRow(
			fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE %s = ?)", table.name, table.pkCol),
			values[0],
		).Scan(&exists)
		if err != nil {
			return err
		}

		if !exists {
			if _, err := deleteStmt.Exec(values[0]); err != nil {
				return err
			}
		}
	}
	return nil
}
