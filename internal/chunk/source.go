package chunk

type Source[T any] interface {
	ID() string
	Next() (T, int64, bool)
	Total() (int64, error)
}

type Merger[T any] interface {
	Merge(src ...Source[T])
}
