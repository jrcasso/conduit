package transform

type Transformer interface {
	Transform(Data string) string
}

type Transform interface{}
