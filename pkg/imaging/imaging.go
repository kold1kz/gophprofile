package imaging

import (
	"bytes"
	"errors"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"

	"golang.org/x/image/draw"
	"golang.org/x/image/webp"
)

// ErrUnsupportedFormat возвращается, когда файл не похож на JPEG, PNG или WEBP.
var ErrUnsupportedFormat = errors.New("unsupported image format")

// ImageInfo хранит результат декодирования картинки: тип, размеры, байты и готовый image.Image.
type ImageInfo struct {
	MimeType string
	Width    int
	Height   int
	Data     []byte
	Image    image.Image
}

// Decode читает изображение целиком, проверяет формат и возвращает данные для дальнейшей обработки.
func Decode(r io.Reader) (ImageInfo, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return ImageInfo{}, err
	}
	mimeType := http.DetectContentType(data)
	var img image.Image

	switch mimeType {
	case "image/jpeg":
		img, err = jpeg.Decode(bytes.NewReader(data))
	case "image/png":
		img, err = png.Decode(bytes.NewReader(data))
	case "image/webp":
		img, err = webp.Decode(bytes.NewReader(data))
	default:
		return ImageInfo{}, ErrUnsupportedFormat
	}
	if err != nil {
		return ImageInfo{}, err
	}

	bounds := img.Bounds()
	return ImageInfo{
		MimeType: mimeType,
		Width:    bounds.Dx(),
		Height:   bounds.Dy(),
		Data:     data,
		Image:    img,
	}, nil
}

// ResizeJPEG масштабирует картинку в заданный размер и кодирует результат в JPEG.
func ResizeJPEG(img image.Image, width, height int) ([]byte, error) {
	dst := image.NewRGBA(image.Rect(0, 0, width, height))
	draw.CatmullRom.Scale(dst, dst.Bounds(), img, img.Bounds(), draw.Over, nil)

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 85}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
