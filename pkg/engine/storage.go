package engine

import (
	"github.com/pkg/errors"
)

var (
	ErrorNotFound = errors.New("object not found")
)

type StorageEngine interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Close()
}
