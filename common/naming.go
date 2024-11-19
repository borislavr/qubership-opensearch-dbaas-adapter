package common

// TODO use naming adapter in basic package
type IndexAdapter struct {
	defaultPrefix string
	delimiter     string
}

func NewIndexAdapter() *IndexAdapter {
	return &IndexAdapter{
		defaultPrefix: "dbaas",
		delimiter:     "_",
	}
}

func (adapter IndexAdapter) NameIndex() string {
	return adapter.NameIndexPrefixed(adapter.defaultPrefix)
}

func (adapter IndexAdapter) NameIndexPrefixed(prefix string) string {
	if prefix == "" {
		prefix = adapter.defaultPrefix
	}
	return prefix + adapter.delimiter + GetUUID()
}
