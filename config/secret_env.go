package config

import (
	"fmt"
	"os"
	"strings"
)

const envSecretStore = "env"

// LinkEnv resolves @{env:NAME} references in the secret to the value of the
// environment variable NAME. Categraf has no secret stores, so without this
// such references stay unlinked and Get fails. References to any other store
// are left alone and still make Get fail. The variables are read once, here.
func (s *Secret) LinkEnv() error {
	unlinked := s.GetUnlinked()
	if len(unlinked) == 0 {
		return nil
	}
	resolvers := make(map[string]ResolveFunc, len(unlinked))
	var missing []string
	for _, ref := range unlinked {
		store, name := splitLink(ref)
		if store != envSecretStore {
			return fmt.Errorf("unsupported secret store %q in %s; only @{env:NAME} is supported", store, ref)
		}
		value, ok := os.LookupEnv(name)
		if !ok {
			missing = append(missing, name)
			continue
		}
		resolvers[ref] = func() ([]byte, bool, error) { return []byte(value), false, nil }
	}
	if len(missing) > 0 {
		return fmt.Errorf("environment variable not set: %s", strings.Join(missing, ", "))
	}
	return s.Link(resolvers)
}
