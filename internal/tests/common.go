package tests

import (
	"crypto/md5"
	"fmt"
	"hash"
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"
)

func GenerateCleanTmpFilePath(t testing.TB, name string) (string, error) {
	databasePath := path.Join(os.TempDir(), name+"__"+strconv.Itoa(rand.Int()))
	t.Log(databasePath)
	_, err := os.Stat(databasePath)
	if !os.IsNotExist(err) {
		return databasePath, os.Remove(databasePath)
	}
	return databasePath, nil
}

type keyValueChecker struct {
	hash hash.Hash
}

func NewKeyValueChecker() *keyValueChecker {
	return &keyValueChecker{hash: md5.New()}
}

func (c *keyValueChecker) GenValue(key []byte) []byte {
	defer c.hash.Reset()
	return []byte(fmt.Sprintf("%x", c.hash.Sum(key)))
}

func (c *keyValueChecker) CheckValue(key, value []byte) bool {
	return string(c.GenValue(key)) == string(value)
}