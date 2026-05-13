package imaging

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeAndResizePNG(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 20, 10))
	img.Set(1, 1, color.White)
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, img))

	info, err := Decode(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, "image/png", info.MimeType)
	require.Equal(t, 20, info.Width)
	require.Equal(t, 10, info.Height)

	thumb, err := ResizeJPEG(info.Image, 5, 5)
	require.NoError(t, err)
	require.NotEmpty(t, thumb)
}
