package simple_node

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/paragor/parabase/internal/tests"
)

func Test_simpleNode_readAndWrite(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	node := SimpleNode{}
	kvChecker := tests.NewKeyValueChecker()
	key := []byte("key_with_name")
	node.SetKey(key)
	node.SetValue(kvChecker.GenValue(key))

	err := node.Write(buf)
	if err != nil {
		t.Error(err)
	}
	newNode := SimpleNode{}
	err = newNode.Read(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(node, newNode) {
		t.Errorf("SimpleNode not equals \ngot  %#v\nwant %#v", newNode, node)
	}

}
