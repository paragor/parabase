package simple_mmap

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/paragor/parabase/internal/tests"
)

func Test_simpleNode_readAndWrite(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	node := simpleNode{}
	kvChecker := tests.NewKeyValueChecker()
	key := []byte("key_with_name")
	node.SetKey(key)
	node.SetValue(kvChecker.GenValue(key))

	err := node.Write(buf)
	if err != nil {
		t.Error(err)
	}
	newNode := simpleNode{}
	err = newNode.Read(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(node, newNode) {
		t.Errorf("simpleNode not equals \ngot  %#v\nwant %#v", newNode, node)
	}

}
