// Package metadata reads possible schema dependencies. It never claims execution.
package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Object struct {
	Name    string
	Type    int
	Package string
}
type Edge struct {
	From, To Object
	Field    string
}
type Graph struct {
	Source, Executed, Correlation, Scope string
	Root                                 Object
	Nodes                                []Object
	Edges                                []Edge
	Truncated                            bool
}
type Config struct {
	Database, SchemaVersion        string
	TTL                            time.Duration
	MaxEntries, MaxNodes, MaxDepth int
}
type key struct {
	database, version string
	root              Object
}
type entry struct {
	graph   Graph
	expires time.Time
}
type Reader struct {
	db         *sql.DB
	c          Config
	mu         sync.Mutex
	cache      map[key]entry
	generation uint64
}

func New(db *sql.DB, c Config) (*Reader, error) {
	if db == nil || c.Database == "" || len(c.Database) > 256 || len(c.SchemaVersion) > 256 {
		return nil, errors.New("metadata: database and bounded logical identity required")
	}
	if c.TTL == 0 {
		c.TTL = time.Minute
	}
	if c.MaxEntries == 0 {
		c.MaxEntries = 64
	}
	if c.MaxNodes == 0 {
		c.MaxNodes = 128
	}
	if c.MaxDepth == 0 {
		c.MaxDepth = 8
	}
	if c.TTL < 0 || c.TTL > time.Hour || c.MaxEntries < 1 || c.MaxEntries > 1024 || c.MaxNodes < 1 || c.MaxNodes > 4096 || c.MaxDepth < 1 || c.MaxDepth > 64 {
		return nil, errors.New("metadata: invalid bounds")
	}
	return &Reader{db: db, c: c, cache: make(map[key]entry)}, nil
}

// Invalidate advances schema identity and prevents in-flight old reads repopulating the cache.
func (r *Reader) Invalidate(version string) error {
	if len(version) > 256 {
		return errors.New("metadata: schema version too long")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.c.SchemaVersion = version
	r.generation++
	clear(r.cache)
	return nil
}
func clone(g Graph) Graph {
	g.Nodes = append([]Object(nil), g.Nodes...)
	g.Edges = append([]Edge(nil), g.Edges...)
	return g
}
func (r *Reader) Read(ctx context.Context, root Object) (Graph, error) {
	g := Graph{Source: "metadata", Executed: "unknown", Correlation: "unmatched", Root: root, Scope: "object"}
	if root.Name == "" || len(root.Name) > 252 || len(root.Package) > 252 || root.Type < 0 || root.Type > 255 {
		return g, errors.New("metadata: invalid object identity")
	}
	r.mu.Lock()
	k := key{r.c.Database, r.c.SchemaVersion, root}
	generation := r.generation
	if e, ok := r.cache[k]; ok && time.Now().Before(e.expires) {
		r.mu.Unlock()
		return clone(e.graph), nil
	}
	r.mu.Unlock()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return g, fmt.Errorf("metadata: begin snapshot: %w", err)
	}
	defer tx.Rollback()
	type item struct {
		o     Object
		depth int
	}
	queue := []item{{root, 0}}
	seen := map[Object]bool{root: true}
	g.Nodes = append(g.Nodes, root)
	if root.Package != "" && (root.Type == 5 || root.Type == 15) {
		body := Object{Name: root.Package, Type: 19}
		queue = []item{{body, 0}}
		seen[body] = true
		g.Nodes = append(g.Nodes, body)
		g.Scope = "package_body"
		if len(g.Nodes) > r.c.MaxNodes {
			g.Nodes = g.Nodes[:r.c.MaxNodes]
			g.Truncated = true
			return g, nil
		}
	}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current.depth >= r.c.MaxDepth {
			g.Truncated = true
			continue
		}
		// FIRST is a validated integer, not caller SQL. Fetch one sentinel for truncation.
		query := fmt.Sprintf(`SELECT FIRST %d RDB$DEPENDED_ON_NAME, RDB$DEPENDED_ON_TYPE, RDB$FIELD_NAME, RDB$PACKAGE_NAME FROM RDB$DEPENDENCIES WHERE RDB$DEPENDENT_NAME = ? AND RDB$DEPENDENT_TYPE = ? ORDER BY RDB$DEPENDED_ON_TYPE, RDB$DEPENDED_ON_NAME`, r.c.MaxNodes+1)
		rows, err := tx.QueryContext(ctx, query, current.o.Name, current.o.Type)
		if err != nil {
			return g, fmt.Errorf("metadata: read dependencies: %w", err)
		}
		scanned := 0
		for rows.Next() {
			if scanned == r.c.MaxNodes {
				g.Truncated = true
				break
			}
			scanned++
			var name string
			var typ int
			var field, pkg sql.NullString
			if err := rows.Scan(&name, &typ, &field, &pkg); err != nil {
				rows.Close()
				return g, err
			}
			// RDB$PACKAGE_NAME qualifies the depended-on routine. Package bodies/headers
			// are first-class object types; never merge same-named packaged and standalone routines.
			to := Object{Name: strings.TrimSpace(name), Type: typ, Package: strings.TrimSpace(pkg.String)}
			if len(g.Edges) >= r.c.MaxNodes*4 || (!seen[to] && len(g.Nodes) >= r.c.MaxNodes) {
				g.Truncated = true
				break
			}
			g.Edges = append(g.Edges, Edge{From: current.o, To: to, Field: strings.TrimSpace(field.String)})
			if !seen[to] {
				seen[to] = true
				g.Nodes = append(g.Nodes, to)
				next := to
				// Packaged routine bodies are stored as dependencies of the package body.
				if to.Package != "" && (to.Type == 5 || to.Type == 15) {
					next = Object{Name: to.Package, Type: 19}
					if !seen[next] {
						if len(g.Nodes) >= r.c.MaxNodes {
							g.Truncated = true
							continue
						}
						seen[next] = true
						g.Nodes = append(g.Nodes, next)
					} else {
						continue
					}
				}
				queue = append(queue, item{next, current.depth + 1})
			}
		}
		err = rows.Err()
		closeErr := rows.Close()
		if err != nil {
			return g, err
		}
		if closeErr != nil {
			return g, closeErr
		}
	}
	if err := tx.Commit(); err != nil {
		return g, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if generation == r.generation {
		now := time.Now()
		for key, e := range r.cache {
			if now.After(e.expires) {
				delete(r.cache, key)
			}
		}
		if len(r.cache) >= r.c.MaxEntries {
			var oldest key
			var when time.Time
			for key, e := range r.cache {
				if when.IsZero() || e.expires.Before(when) {
					oldest = key
					when = e.expires
				}
			}
			delete(r.cache, oldest)
		}
		r.cache[k] = entry{clone(g), now.Add(r.c.TTL)}
	}
	return g, nil
}
