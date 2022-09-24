package main

import (
	"golang.org/x/tour/pic"
	"image"
	"image/color"
)

type Image struct{
	height int
	width int
}

func (self Image) ColorModel() color.Model {
	return color.RGBAModel
}
func (self Image) Bounds() image.Rectangle {
	return image.Rect(0, 0, self.width, self.height)
}

func (self Image) At(x, y int) color.Color {
	return color.RGBA{uint8(x) * uint8(y), uint8(x) * uint8(y), 255, 255}
}

func main() {
	m := Image{255, 255}
	pic.ShowImage(m)
}
