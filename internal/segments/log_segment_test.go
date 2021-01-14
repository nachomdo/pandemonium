package segments

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"pingcap.com/kvs/internal/segments/encoding"
)

func dummyLogSegment(t *testing.T, data map[string][]byte) string {
	fd, err := ioutil.TempFile("/tmp", "segment_*.dat")
	assert.NoError(t, err)
	defer fd.Close()
	encoder := encoding.NewBitCaskEncoder(fd)
	for k, v := range data {
		_, err := encoder.Write([]byte(k), v)
		assert.NoError(t, err)
	}
	return fd.Name()
}
func TestReadAllSegment(t *testing.T) {
	tmpSegment := dummyLogSegment(t, map[string][]byte{
		"1": []byte("coffee"),
		"2": []byte("tea"),
		"3": []byte("nuts"),
		"4": []byte("pastry")})
	ls, err := NewLogSegment(tmpSegment, false)
	assert.NoError(t, err)

	kdt, err := ls.ReadAll()
	assert.NoError(t, err)
	assert.NotNil(t, kdt)
}

func TestReadAtSegment(t *testing.T) {
	entryExpectedData := map[string][]byte{
		"1": []byte("coffee"),
		"2": []byte("tea"),
		"3": []byte("nuts"),
		"4": []byte("pastry"),
	}
	tmpSegment := dummyLogSegment(t, entryExpectedData)
	ls, err := NewLogSegment(tmpSegment, false)
	assert.NoError(t, err)
	kdt, err := ls.ReadAll()
	assert.NoError(t, err)
	for k, v := range kdt.Data {
		rk, rv, err := ls.ReadAt(v.Offset, v.Size)
		assert.NoError(t, err)
		assert.Equal(t, k, string(rk))
		assert.Equal(t, entryExpectedData[k], rv)
	}
}

func TestAppendToSegment(t *testing.T) {
	entryExpectedData := map[string][]byte{
		"1": []byte("coffee"),
	}
	tmpSegment := dummyLogSegment(t, entryExpectedData)
	ls, err := NewLogSegment(tmpSegment, true)
	assert.NoError(t, err)
	kdt, err := ls.ReadAll()
	assert.NoError(t, err)
	assert.NotNil(t, kdt)
	kdt.Data["2"], err = ls.Write([]byte("2"), []byte("kombucha"))
	assert.NoError(t, err)

	kdt.Data["3"], err = ls.Write([]byte("3"), []byte("oat milk"))
	assert.NoError(t, err)

	_, v, err := ls.ReadAt(kdt.Data["2"].Offset, kdt.Data["2"].Size)
	assert.NoError(t, err)
	assert.Equal(t, []byte("kombucha"), v)

	_, v, err = ls.ReadAt(kdt.Data["3"].Offset, kdt.Data["3"].Size)
	assert.NoError(t, err)
	assert.Equal(t, []byte("oat milk"), v)

}

func TestSegmentID(t *testing.T) {

	data := []struct {
		input    string
		active   bool
		expected int
	}{
		{
			input:    "segment_00015.dat",
			expected: 15,
		},
		{
			input:    "segment_00002.dat",
			expected: 2,
		},
		{
			input:    "/tmp/current.dat",
			active:   true,
			expected: 1,
		},
	}
	for _, item := range data {
		assert.Equal(t, item.expected, SegmentID(item.input, item.active))
	}
}
