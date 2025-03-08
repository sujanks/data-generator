package sink

import (
	"log"
	"time"

	"github.com/go-pg/pg/v10"
)

type pgDataSink struct {
	db      pg.DB
	profile string
}

// InsertRecord implements DataSink.
func (pgDataSink *pgDataSink) InsertRecord(tableName string, data map[string]interface{}) error {
	panic("unimplemented")
}

func NewPgDataSink(p string) DataSink {
	return &pgDataSink{
		db:      *pgConnection(),
		profile: p,
	}
}

func pgConnection() *pg.DB {
	opts := &pg.Options{
		Addr:     "db:5432",
		User:     "user",
		Password: "user",
		Database: "postgres",
	}

	var db *pg.DB
	for i := 0; i < 10; i++ {
		db = pg.Connect(opts)
		if _, err := db.Exec("SELECT 1"); err == nil {
			return db
		}
		time.Sleep(2 * time.Second)
	}
	log.Fatal("could not connect to postgres database")
	return nil
}

func (pgDataSink *pgDataSink) InsertRcord(tableName string, roKeyValue map[string]interface{}) {
	_, err := pgDataSink.db.Model(&roKeyValue).TableExpr(tableName).Insert()
	if err != nil {
		log.Fatalf("Failed to inert %v", err)
	}
}
