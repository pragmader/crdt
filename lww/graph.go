package lww

import (
	"sort"
	"sync"

	"github.com/pkg/errors"
)

var (
	// ErrVertexAlreadyExists occurs when trying to add a vertex
	// with a key that already exists in the graph
	ErrVertexAlreadyExists = errors.New("vertex already exists in the graph")
	// ErrInvalidVertexType occurs when the internal data has a wrong structure
	ErrInvalidVertexType = errors.New("invalid vertex type")
	// ErrVertexNotFound occurs when trying to access a non existing vertex key
	ErrVertexNotFound = errors.New("vertex not found")
	// ErrPathNotFound occurs when there is no path between the given vertices
	ErrPathNotFound = errors.New("path not found")
)

// nothing is a type with zero memory allocation.
// It's used for marking visited vertices in a efficient way.
type nothing struct{}

// Vertex is a graph vertex that holds a unique key and a value
type Vertex struct {
	// Key is a universally unique identifier (e.g. UUID v4) of the vertex
	Key string
	// Value is an arbitrary value stored in the vertex
	Value string
}

// GetKey implements the `Element` interface
func (v Vertex) GetKey() string {
	return v.Key
}

// VertexWithEdges is a struct that contains a vertex and its adjacent keys.
// It's used for a flat export of the graph data, so it can be compared in tests for example
type VertexWithEdges struct {
	// Vertex itself
	Vertex
	// AdjacentKeys is a list of adjacent vertex keys.
	AdjacentKeys []string
}

// NewGraph initializes the Last-Writer-Wins state-based graph and makes it ready for use.
func NewGraph() Graph {
	return Graph{
		mutex:    &sync.Mutex{},
		vertices: NewSet(),
		edges:    make(map[string]Set),
	}
}

// Graph is a Last-Writer-Wins state-based directional graph.
// Use `NewGraph` in order to initialize it before use.
// The graph is thread-safe and can be used from several go routines.
//
// The implementation is basically composing two dimensions of LWW sets into a graph data structure:
// * 1st dimension is a set of vertices
// * 2nd dimension is a set of edges (adjacent vertices) in each vertex
//
// In this implementation it's possible to have a hanging edge that points to a
// vertex that have been removed. It's properly handled throughout the code.
//
// It enables a better user-experience when a vertex that has an edge is removed and re-added on
// several nodes, with this approach the edge would not be lost.
//
// Consider the following scenario:
// 1. A adds vertex V1
// 2. B and A replicate
// 3. B adds vertex V2 and edge (V1, V2)
// 4. A removes V1
// 5. C adds vertex V1
// 6. B and A replicate: V1 gets removed from B, despite that the edge V1->V2 is kept
// 7. C and B replicate:
//   7.1 V1 gets re-added to B
//   7.2 the edge V1->V2 gets restored in B and replicated to C
//   7.3 Both B and C now have vertices V1 and V2 and the edge V1->V2
// 8. A and B (or C) replicate: A now also has vertices V1, V2 and the edge V1->V2 because of LWW
//
// Time ->
// A--AddVertex(V1)-\-RemoveVertex(V1)---------------\----\-|
// B-----------------\-AddVertex(V2),AddEdge(V1, V2)--\-\--\|=> A,B,C = {V1->V2}
// C---------------------------AddVertex(V1)-------------\--|
type Graph struct {
	// mutex is used for the thread-safety
	mutex *sync.Mutex

	// vertices is a Last-Writer-Wins state-based element set of all the graph vertices
	vertices Set

	// edges is a map from a vertex key to a Last-Writer-Wins state-based
	// element set of all keys of adjacent vertices
	edges map[string]Set
}

// AddVertex adds the given vertex `v` to the graph.
// Returns `ErrVertexAlreadyExists` if a vertex with the same key already exists in the graph.
func (g Graph) AddVertex(v Vertex) error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	_, err := g.Lookup(v.Key)
	if err == nil {
		return ErrVertexAlreadyExists
	}
	if !errors.Is(err, ErrVertexNotFound) {
		return err
	}

	g.vertices.Add(v)

	return nil
}

// RemoveVertex removes the vertex with the given key.
// Returns an error with `ErrVertexNotfound` cause if
// the vertex with the given key does not exist
func (g Graph) RemoveVertex(key string) (err error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	_, err = g.Lookup(key)
	if err != nil {
		return err
	}

	g.vertices.Remove(key)

	return nil
}

// AddEdge adds a directional edge from a vertex with `fromKey` to a vertex with `toKey`.
// Returns an error with `ErrVertexNotfound` cause if
// one of the vertices with the given key does not exist
func (g Graph) AddEdge(fromKey, toKey string) error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	_, err := g.Lookup(fromKey)
	if err != nil {
		return err
	}

	_, err = g.Lookup(toKey)
	if err != nil {
		return err
	}

	adjacent := g.getAdjacent(fromKey)
	adjacent.Add(IDElement(toKey))

	return nil
}

// AddEdge removes a directional edge from a vertex with `fromKey` to a vertex with `toKey`.
// Returns an error with `ErrVertexNotfound` cause if
// one of the vertices with the given key does not exist
func (g Graph) RemoveEdge(fromKey, toKey string) error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	_, err := g.Lookup(fromKey)
	if err != nil {
		return err
	}

	_, err = g.Lookup(toKey)
	if err != nil {
		return err
	}

	adjacent := g.getAdjacent(fromKey)
	adjacent.Remove(toKey)

	return nil
}

// Lookup checks if a vertex with the given key exists in the graph.
// Returns the found vertex and no error if the vertex exists.
// Returns an error with `ErrVertexNotfound` cause if
// the vertex with the given key does not exist
func (g Graph) Lookup(key string) (found Vertex, err error) {
	// no lock required, we access only `vertices` set and it's thread-safe
	foundElement, err := g.vertices.Lookup(key)
	if errors.Is(err, ErrElementNotFound) {
		return found, errors.Wrapf(ErrVertexNotFound, "failed to find vertex [key = %q]", key)
	}
	if err != nil {
		return found, err
	}

	switch v := foundElement.(type) {
	case Vertex:
		return v, nil
	default:
		return found, errors.Wrapf(ErrInvalidVertexType, "vertex [key = %q] is of invalid type", key)
	}
}

// FindConnected returns a list of vertices which are connected to the vertex with the given key.
//
// Vertex V1 is considered connected to vertex Vn only when there is a directed path from V1 to Vn:
// * V1->V2->V3 - V1 is connected to V3
// * V1->V2<-V3 - V1 is not connected to V3
//
// The resulting list order is breadth-first, however,
// because of the internally used map the order in the result list is
// not deterministic within a single adjacent vertex set.
func (g Graph) FindConnected(key string) (connected []Vertex, err error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	start, err := g.Lookup(key)
	if err != nil {
		return nil, err
	}

	// it's always at least an empty list
	connected = []Vertex{}

	// breadth-first traversal

	// a set to mark visited vertices
	visited := make(map[string]nothing)
	// the traversal queue for BFS
	queue := []Vertex{start}

	var current Vertex

	for {
		if len(queue) == 0 {
			return connected, nil
		}

		// dequeue
		current = queue[0]
		queue = queue[1:]

		adjacent := g.getAdjacent(current.Key)
		for _, v := range adjacent.List() {
			// some edges exist even for removed vertices
			vertex, err := g.Lookup(v.GetKey())
			if errors.Is(err, ErrVertexNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}

			_, toSkip := visited[vertex.Key]
			if toSkip {
				continue
			}
			visited[vertex.Key] = nothing{}

			connected = append(connected, vertex)
			queue = append(queue, vertex)
		}
	}
}

// FindPath returns a list of vertices that construct a path between
// a vertex with the key `fromKey` and a vertex with the key `toKey`.
//
// Returns a list of vertices and no error if there is a path from
// `fromKey` to `toKey`.
// Returns `nil` and `ErrPathNotFound` when the vertices are not connected.
//
// The resulted path always starts with the "from" vertex and ends with the "to" vertex.
// The path can also start and end with the same vertex if there is a loop on the way.
//
// Because of the data internals the result is not guarantied to be deterministic.
func (g Graph) FindPath(fromKey, toKey string) (path []Vertex, err error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	start, err := g.Lookup(fromKey)
	if err != nil {
		return nil, err
	}

	_, err = g.Lookup(toKey)
	if err != nil {
		return nil, err
	}

	// depth-first traversal with path tracing

	// a set to mark keys of visited vertices
	visited := make(map[string]nothing)
	path = []Vertex{start}

	return g.findPath(start, toKey, path, visited)
}

// findPath performs a single recursive iteration of DFS in the `FindPath` function.
func (g Graph) findPath(start Vertex, searchKey string, currentPath []Vertex, visited map[string]nothing) (path []Vertex, err error) {
	_, toSkip := visited[start.Key]
	if toSkip {
		return nil, ErrPathNotFound
	}
	visited[start.Key] = nothing{}

	adjacent := g.getAdjacent(start.Key).List()
	for _, v := range adjacent {
		// some edges exist even for removed vertices
		vertex, err := g.Lookup(v.GetKey())
		if errors.Is(err, ErrVertexNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if vertex.Key == searchKey {
			return append(currentPath, vertex), nil
		}

		path, err = g.findPath(vertex, searchKey, append(currentPath, vertex), visited)
		if errors.Is(err, ErrPathNotFound) {
			continue
		}
		return path, err
	}

	return nil, ErrPathNotFound
}

// List returns a comparable graph representation.
// This function produces deterministic results.
func (g Graph) List() (list []VertexWithEdges, err error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	list = []VertexWithEdges{}

	vertices := g.vertices.List()
	sort.Slice(vertices, func(i, j int) bool {
		return vertices[i].GetKey() < vertices[j].GetKey()
	})

	for _, element := range vertices {
		vertex, err := g.Lookup(element.GetKey())
		if err != nil {
			return nil, err
		}
		adjacent := g.getAdjacent(vertex.Key).List()
		vwe := VertexWithEdges{
			Vertex:       vertex,
			AdjacentKeys: make([]string, 0, len(adjacent)),
		}

		for _, adjacentKey := range adjacent {
			vwe.AdjacentKeys = append(vwe.AdjacentKeys, adjacentKey.GetKey())
		}
		sort.Strings(vwe.AdjacentKeys)

		list = append(list, vwe)
	}

	return list, nil
}

// Merge takes another LWW Graph as a `remote` and merges its state into itself.
// Merging two replicas takes the union of the respective vertices and edges.
func (g Graph) Merge(remote Graph) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	// replicating vertices
	g.vertices.Merge(remote.vertices)

	// replicating edges
	for vertexKey, remoteAdjacent := range remote.edges {
		localAdjacent := g.getAdjacent(vertexKey)
		localAdjacent.Merge(remoteAdjacent)
	}
}

// getAdjacent returns an LWW Element Set of keys of adjacent vertices.
// This function also initializes the set of adjacent keys if needed.
func (g Graph) getAdjacent(vertexKey string) Set {
	// if these vertex edges are being requested for the first time,
	// we need to initialize the set
	edges, edgesExist := g.edges[vertexKey]
	if !edgesExist {
		edges = NewSet()
		g.edges[vertexKey] = edges
	}
	return edges
}
