package main

import (
	"path/filepath"
	"testing"
)

func TestQueryPublishesFileReferenceAndKeepsOpenDefault(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// The file does not have to be opened to derive its MIME type.
	const path = "/pictures/sky.PNG"
	if _, err := db.Exec("INSERT INTO files VALUES (?, ?, 0, 12)", path, "sky.PNG"); err != nil {
		t.Fatal(err)
	}
	rows, err := (&indexer{db: db}).query(t.Context(), "sky")
	if err != nil || len(rows) != 1 {
		t.Fatalf("query: %+v, %v", rows, err)
	}
	row := rows[0]
	if row.ID != path || row.Ref != (fileRef{Type: "file", Value: path, MIME: "image/png"}) ||
		len(row.Actions) != 2 || row.Actions[0].Name != "open" || !row.Actions[0].Default || row.Actions[1].Name != "open_folder" {
		t.Fatalf("file row: %+v", row)
	}
}
