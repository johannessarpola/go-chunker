package chunk

type Output struct {
	Prefix string
	Dir    string
	Ext    string
}

func NewOutput(prefix, dir, ext string) Output {
	return Output{Prefix: prefix, Dir: dir, Ext: ext}
}
