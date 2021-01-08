package main

import (
	"log"
	"os"
	"path"

	"github.com/paragor/parabase/pkg/serverd"
	"github.com/paragor/parabase/pkg/storage_impl/with_hashtable_index_mmap"
)

func main() {
	log.Println("starting...")

	engine, err := with_hashtable_index_mmap.NewStorage(path.Join(os.TempDir(), "parabase.db"))
	if err != nil {
		log.Fatalf("cant create engine: %s", err.Error())
	}
	defer engine.Close()

	server := serverd.NewServerd(engine)
	log.Fatal(server.Run())
}
