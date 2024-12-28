package chunk

type Source[T any] interface {
	Next() (T, int64, bool)
	Total() (int64, error)
}

type Merger[T any] interface {
	Merge(src ...Source[T])
}
