package engine

import (
	"github.com/pkg/errors"
)

var (
	NotFoundError = errors.New("object not found")
)

type StorageEngine interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Close()
}
