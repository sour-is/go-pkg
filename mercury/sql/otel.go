package sql

import (
	"database/sql"
	"strings"

	"go.nhat.io/otelsql"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
)

func openDB(driver, dsn string) (*sql.DB, error) {
	system := semconv.DBSystemPostgreSQL
	if driver == "sqlite" || strings.HasPrefix(driver, "libsql") {
		system = semconv.DBSystemSqlite
	}

	if driver == "postgres" {
		var err error
		// Register the otelsql wrapper for the provided postgres driver.
		driver, err = otelsql.Register(driver,
			otelsql.AllowRoot(),
			otelsql.TraceQueryWithoutArgs(),
			otelsql.TraceRowsClose(),
			otelsql.TraceRowsAffected(),
			// otelsql.WithDatabaseName("my_database"),        // Optional.
			otelsql.WithSystem(system), // Optional.
		)
		if err != nil {
			return nil, err
		}
	}

	// Connect to a Postgres database using the postgres driver wrapper.
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}

	if err := otelsql.RecordStats(db); err != nil {
		return nil, err
	}

	return db, nil
}
