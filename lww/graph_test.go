package lww

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGraph(t *testing.T) {
	t.Run("CRDT properties", func(t *testing.T) {
		v1 := Vertex{Key: "vertex1", Value: "value1"}
		v2 := Vertex{Key: "vertex2", Value: "value2"}
		v3 := Vertex{Key: "vertex3", Value: "value3"}
		v4 := Vertex{Key: "vertex4", Value: "value4"}

		t.Run("Eventual convergence", func(t *testing.T) {
			// copies of shared objects are identical at all sites if updates
			// cease and all generated updates are propagated to all sites.

			t.Run("all actors converge to the same state after replication", func(t *testing.T) {
				// Time ->
				// A--AddVertex(v1)----------------------------------\---|
				// B------AddVertex(v2),AddVertex(v3),AddEdge(v2, v3)-\--|=> A,B,C = {v1, v2->v3, v4}
				// C------------------------------------AddVertex(v4)--\-|

				A := NewGraph()
				B := NewGraph()
				C := NewGraph()

				err := A.AddVertex(v1)
				require.NoError(t, err)

				err = B.AddVertex(v2)
				require.NoError(t, err)
				err = B.AddVertex(v3)
				require.NoError(t, err)
				err = B.AddEdge(v2.Key, v3.Key)
				require.NoError(t, err)

				err = C.AddVertex(v4)
				require.NoError(t, err)

				replicateGraphs(A, B, C)
				equalGraphs(t, A, B, C)
			})
		})

		t.Run("Intention-preservation", func(t *testing.T) {
			// for any update O, the effect of executing O at all sites is the same
			// as the intention of O when executed at the site that originated it,
			// and the effect of executing O does not change the effect of non concurrent operations.

			t.Run("edge for a removed vertex re-appears if the vertex was re-added in another replica", func(t *testing.T) {
				// 1. A adds vertex v1
				// 2. B and A replicate
				// 3. B adds vertex v2 and edge (v1, v2)
				// 4. A removes v1
				// 5. C adds vertex v1
				// 6. B and A replicate: v1 gets removed from B, despite that the edge v1->v2 is kept
				// 7. C and B replicate:
				//   7.1 v1 gets re-added to B
				//   7.2 the edge v1->v2 gets restored in B and replicated to C
				//   7.3 Both B and C now have vertices v1 and v2 and the edge v1->v2
				// 8. A and B (or C) replicate: A now also has vertices v1, v2 and the edge v1->v2 because of LWW
				//
				// Time ->
				// A--AddVertex(v1)-\-RemoveVertex(v1)---------------\----\-|
				// B-----------------\-AddVertex(v2),AddEdge(v1, v2)--\-\--\|=> A,B,C = {v1->v2}
				// C---------------------------AddVertex(v1)-------------\--|

				A := NewGraph()
				B := NewGraph()
				C := NewGraph()

				err := A.AddVertex(v1)
				require.NoError(t, err)

				replicateGraphs(A, B)

				err = A.RemoveVertex(v1.Key)
				require.NoError(t, err)

				err = B.AddVertex(v2)
				require.NoError(t, err)
				err = B.AddEdge(v1.Key, v2.Key)
				require.NoError(t, err)

				err = C.AddVertex(v1)
				require.NoError(t, err)

				replicateGraphs(A, B, C)

				expected := []VertexWithEdges{
					{
						Vertex:       v1,
						AdjacentKeys: []string{v2.Key},
					},
					{
						Vertex:       v2,
						AdjacentKeys: []string{},
					},
				}

				list, err := A.List()
				require.NoError(t, err)
				require.Equal(t, expected, list)

				list, err = B.List()
				require.NoError(t, err)
				require.Equal(t, expected, list)

				list, err = C.List()
				require.NoError(t, err)
				require.Equal(t, expected, list)
			})
		})

		t.Run("Precedence", func(t *testing.T) {
			// if one update Oa causally precedes another update Ob,
			// then, at each site, the execution of Oa happens before the execution of Ob.

			t.Run("same vertices and an edge get re-added after removal", func(t *testing.T) {
				// Time ->
				// A--AddVertex(v1)--------------------RemoveVertex(v1)--------\---|
				// B------AddVertex(v1),AddVertex(v2)----------AddEdge(v1, v2)--\--|=> A,B = {v2}
				A := NewGraph()
				B := NewGraph()

				err := A.AddVertex(v1)
				require.NoError(t, err)

				err = B.AddVertex(v1)
				require.NoError(t, err)
				err = B.AddVertex(v2)
				require.NoError(t, err)

				err = A.RemoveVertex(v1.Key)
				require.NoError(t, err)

				err = B.AddEdge(v1.Key, v2.Key)
				require.NoError(t, err)

				replicateGraphs(A, B)

				expected := []VertexWithEdges{
					{
						Vertex:       v2,
						AdjacentKeys: []string{},
					},
				}

				list, err := A.List()
				require.NoError(t, err)
				require.Equal(t, expected, list)

				list, err = B.List()
				require.NoError(t, err)
				require.Equal(t, expected, list)
			})
		})
	})

	t.Run("Graph operations", func(t *testing.T) {
		t.Run("Vertices", func(t *testing.T) {
			key := "hello"
			vertex := Vertex{
				Key:   key,
				Value: "world",
			}

			t.Run("retreives an added vertex", func(t *testing.T) {
				g := NewGraph()

				err := g.AddVertex(vertex)
				require.NoError(t, err)

				retreived, err := g.Lookup(key)
				require.NoError(t, err)
				require.Equal(t, vertex, retreived)
			})

			t.Run("returns ErrVertexAlreadyExists when adding the same vertex twice", func(t *testing.T) {
				g := NewGraph()

				err := g.AddVertex(vertex)
				require.NoError(t, err)
				err = g.AddVertex(vertex)
				require.ErrorIs(t, err, ErrVertexAlreadyExists)
			})

			t.Run("returns ErrVertexNotFound when retreiving a non-existing vertex", func(t *testing.T) {
				g := NewGraph()

				vertex, err := g.Lookup("non-existing")
				require.ErrorIs(t, err, ErrVertexNotFound)
				require.Empty(t, vertex)
			})

			t.Run("returns ErrVertexNotFound on lookup after removing a vertex", func(t *testing.T) {
				g := NewGraph()

				err := g.AddVertex(vertex)
				require.NoError(t, err)

				err = g.RemoveVertex(vertex.Key)
				require.NoError(t, err)

				vertex, err := g.Lookup(vertex.Key)
				require.ErrorIs(t, err, ErrVertexNotFound)
				require.Empty(t, vertex)
			})

			t.Run("returns ErrVertexNotFound when removing a non-existing vertex", func(t *testing.T) {
				g := NewGraph()

				err := g.RemoveVertex("non-existing")
				require.ErrorIs(t, err, ErrVertexNotFound)
			})
		})

		t.Run("Edges", func(t *testing.T) {
			v1 := Vertex{Key: "vertex1", Value: "value1"}
			v2 := Vertex{Key: "vertex2", Value: "value2"}
			v3 := Vertex{Key: "vertex3", Value: "value3"}
			v4 := Vertex{Key: "vertex4", Value: "value4"}
			v5 := Vertex{Key: "vertex5", Value: "value5"}

			t.Run("connects vertices after adding edges", func(t *testing.T) {
				// v1---->v3---->v5
				//  |     ^
				//  v     |
				// v2     v4
				//
				// v1 is connected to v2, v3, v5
				// v2 is not connected
				// v3 is connected to v5
				// v4 is connected to v3, v5
				// v5 is not connected
				g := NewGraph()

				t.Run("adding vertices", func(t *testing.T) {
					err := g.AddVertex(v1)
					require.NoError(t, err)

					err = g.AddVertex(v2)
					require.NoError(t, err)

					err = g.AddVertex(v3)
					require.NoError(t, err)

					err = g.AddVertex(v4)
					require.NoError(t, err)

					err = g.AddVertex(v5)
					require.NoError(t, err)
				})

				t.Run("adding edges", func(t *testing.T) {
					err := g.AddEdge(v1.Key, v2.Key)
					require.NoError(t, err)

					err = g.AddEdge(v1.Key, v3.Key)
					require.NoError(t, err)

					err = g.AddEdge(v3.Key, v5.Key)
					require.NoError(t, err)

					err = g.AddEdge(v4.Key, v3.Key)
					require.NoError(t, err)
				})

				pathTestCases := []struct {
					name          string
					fromKey       string
					toKey         string
					possiblePaths [][]Vertex
					expectedErr   error
				}{
					// v1
					{
						name:        "path from v1 to v1",
						fromKey:     v1.Key,
						toKey:       v1.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:          "path from v1 to v2",
						fromKey:       v1.Key,
						toKey:         v2.Key,
						possiblePaths: [][]Vertex{{v1, v2}},
					},
					{
						name:          "path from v1 to v3",
						fromKey:       v1.Key,
						toKey:         v3.Key,
						possiblePaths: [][]Vertex{{v1, v3}},
					},
					{
						name:        "path from v1 to v4",
						fromKey:     v1.Key,
						toKey:       v4.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:          "path from v1 to v5",
						fromKey:       v1.Key,
						toKey:         v5.Key,
						possiblePaths: [][]Vertex{{v1, v3, v5}},
					},
					// v2
					{
						name:        "path from v2 to v1",
						fromKey:     v2.Key,
						toKey:       v1.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:        "path from v2 to v2",
						fromKey:     v2.Key,
						toKey:       v2.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:        "path from v2 to v3",
						fromKey:     v2.Key,
						toKey:       v3.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:        "path from v2 to v4",
						fromKey:     v2.Key,
						toKey:       v4.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:        "path from v2 to v5",
						fromKey:     v2.Key,
						toKey:       v5.Key,
						expectedErr: ErrPathNotFound,
					},
					// v3
					{
						name:        "path from v3 to v1",
						fromKey:     v3.Key,
						toKey:       v1.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:        "path from v3 to v2",
						fromKey:     v3.Key,
						toKey:       v2.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:        "path from v3 to v3",
						fromKey:     v3.Key,
						toKey:       v3.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:        "path from v3 to v4",
						fromKey:     v3.Key,
						toKey:       v4.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:          "path from v3 to v5",
						fromKey:       v3.Key,
						toKey:         v5.Key,
						possiblePaths: [][]Vertex{{v3, v5}},
					},
					// v4
					{
						name:        "path from v4 to v1",
						fromKey:     v4.Key,
						toKey:       v1.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:        "path from v4 to v2",
						fromKey:     v4.Key,
						toKey:       v2.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:          "path from v4 to v3",
						fromKey:       v4.Key,
						toKey:         v3.Key,
						possiblePaths: [][]Vertex{{v4, v3}},
					},
					{
						name:        "path from v4 to v4",
						fromKey:     v4.Key,
						toKey:       v4.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:          "path from v4 to v5",
						fromKey:       v4.Key,
						toKey:         v5.Key,
						possiblePaths: [][]Vertex{{v4, v3, v5}},
					},
					// v5
					{
						name:        "path from v5 to v1",
						fromKey:     v5.Key,
						toKey:       v1.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:        "path from v5 to v2",
						fromKey:     v5.Key,
						toKey:       v2.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:        "path from v5 to v3",
						fromKey:     v5.Key,
						toKey:       v3.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:        "path from v5 to v4",
						fromKey:     v5.Key,
						toKey:       v4.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:        "path from v5 to v5",
						fromKey:     v5.Key,
						toKey:       v5.Key,
						expectedErr: ErrPathNotFound,
					},
				}

				for _, tc := range pathTestCases {
					t.Run(tc.name, func(t *testing.T) {
						path, err := g.FindPath(tc.fromKey, tc.toKey)
						if tc.expectedErr != nil {
							require.Error(t, err)
							require.ErrorIs(t, err, tc.expectedErr)
							require.Nil(t, path)
							return
						}
						require.NoError(t, err)
						require.Contains(t, tc.possiblePaths, path)
					})
				}

				connectivityTestCases := []struct {
					name      string
					key       string
					connected []Vertex
				}{
					{
						name:      "v1 is connected to v2, v3, v5",
						key:       v1.Key,
						connected: []Vertex{v2, v3, v5},
					},
					{
						name:      "v2 is not connected",
						key:       v2.Key,
						connected: []Vertex{},
					},
					{
						name:      "v3 is connected to v5",
						key:       v3.Key,
						connected: []Vertex{v5},
					},
					{
						name:      "v4 is connected to v3 and v5",
						key:       v4.Key,
						connected: []Vertex{v3, v5},
					},
					{
						name:      "v5 is not connected",
						key:       v5.Key,
						connected: []Vertex{},
					},
				}

				for _, tc := range connectivityTestCases {
					t.Run(tc.name, func(t *testing.T) {
						connected, err := g.FindConnected(tc.key)
						require.NoError(t, err)
						sortVertices(connected)
						sortVertices(tc.connected)
						require.Equal(t, tc.connected, connected)
					})
				}
			})

			t.Run("returns a vertex which is connected to itself", func(t *testing.T) {
				g := NewGraph()

				err := g.AddVertex(v1)
				require.NoError(t, err)

				err = g.AddEdge(v1.Key, v1.Key)
				require.NoError(t, err)

				connected, err := g.FindConnected(v1.Key)
				require.NoError(t, err)
				require.Equal(t, []Vertex{v1}, connected)

				path, err := g.FindPath(v1.Key, v1.Key)
				require.NoError(t, err)
				require.Equal(t, []Vertex{v1, v1}, path)
			})

			t.Run("resolves loops in the graph", func(t *testing.T) {
				// v1<----v4
				// ^|     ^
				// |v     |
				// v2---->v3+-+
				//          <-+
				// loops:
				// * v1<->v2
				// * v1->v2->v3->v4->v1
				// * v3<->v3
				g := NewGraph()

				t.Run("adding vertices", func(t *testing.T) {
					err := g.AddVertex(v1)
					require.NoError(t, err)

					err = g.AddVertex(v2)
					require.NoError(t, err)

					err = g.AddVertex(v3)
					require.NoError(t, err)

					err = g.AddVertex(v4)
					require.NoError(t, err)
				})

				t.Run("adding edges", func(t *testing.T) {
					err := g.AddEdge(v1.Key, v2.Key)
					require.NoError(t, err)

					err = g.AddEdge(v2.Key, v1.Key)
					require.NoError(t, err)

					err = g.AddEdge(v2.Key, v3.Key)
					require.NoError(t, err)

					err = g.AddEdge(v3.Key, v3.Key)
					require.NoError(t, err)

					err = g.AddEdge(v3.Key, v4.Key)
					require.NoError(t, err)

					err = g.AddEdge(v4.Key, v1.Key)
					require.NoError(t, err)
				})

				t.Run("every vertex is connected", func(t *testing.T) {
					connected, err := g.FindConnected(v1.Key)
					require.NoError(t, err)
					sortVertices(connected)
					require.Equal(t, []Vertex{v1, v2, v3, v4}, connected)
				})

				pathTestCases := []struct {
					name          string
					fromKey       string
					toKey         string
					possiblePaths [][]Vertex
				}{
					// v1
					{
						name:    "path from v1 to v1",
						fromKey: v1.Key,
						toKey:   v1.Key,
						possiblePaths: [][]Vertex{
							{v1, v2, v1},
							{v1, v2, v3, v4, v1},
						},
					},
					{
						name:          "path from v1 to v2",
						fromKey:       v1.Key,
						toKey:         v2.Key,
						possiblePaths: [][]Vertex{{v1, v2}},
					},
					{
						name:          "path from v1 to v3",
						fromKey:       v1.Key,
						toKey:         v3.Key,
						possiblePaths: [][]Vertex{{v1, v2, v3}},
					},
					{
						name:          "path from v1 to v4",
						fromKey:       v1.Key,
						toKey:         v4.Key,
						possiblePaths: [][]Vertex{{v1, v2, v3, v4}},
					},
					// v2
					{
						name:    "path from v2 to v1",
						fromKey: v2.Key,
						toKey:   v1.Key,
						possiblePaths: [][]Vertex{
							{v2, v1},
							{v2, v3, v4, v1},
						},
					},
					{
						name:    "path from v2 to v2",
						fromKey: v2.Key,
						toKey:   v2.Key,
						possiblePaths: [][]Vertex{
							{v2, v1, v2},
							{v2, v3, v4, v1, v2},
						},
					},
					{
						name:          "path from v2 to v3",
						fromKey:       v2.Key,
						toKey:         v3.Key,
						possiblePaths: [][]Vertex{{v2, v3}},
					},
					{
						name:          "path from v2 to v4",
						fromKey:       v2.Key,
						toKey:         v4.Key,
						possiblePaths: [][]Vertex{{v2, v3, v4}},
					},
					// v3
					{
						name:          "path from v3 to v1",
						fromKey:       v3.Key,
						toKey:         v1.Key,
						possiblePaths: [][]Vertex{{v3, v4, v1}},
					},
					{
						name:          "path from v3 to v2",
						fromKey:       v3.Key,
						toKey:         v2.Key,
						possiblePaths: [][]Vertex{{v3, v4, v1, v2}},
					},
					{
						name:    "path from v3 to v3",
						fromKey: v3.Key,
						toKey:   v3.Key,
						possiblePaths: [][]Vertex{
							{v3, v3},
							{v3, v4, v1, v2, v3},
						},
					},
					{
						name:          "path from v3 to v4",
						fromKey:       v3.Key,
						toKey:         v4.Key,
						possiblePaths: [][]Vertex{{v3, v4}},
					},
					// v4
					{
						name:          "path from v4 to v1",
						fromKey:       v4.Key,
						toKey:         v1.Key,
						possiblePaths: [][]Vertex{{v4, v1}},
					},
					{
						name:          "path from v4 to v2",
						fromKey:       v4.Key,
						toKey:         v2.Key,
						possiblePaths: [][]Vertex{{v4, v1, v2}},
					},
					{
						name:          "path from v4 to v3",
						fromKey:       v4.Key,
						toKey:         v3.Key,
						possiblePaths: [][]Vertex{{v4, v1, v2, v3}},
					},
					{
						name:          "path from v4 to v4",
						fromKey:       v4.Key,
						toKey:         v4.Key,
						possiblePaths: [][]Vertex{{v4, v1, v2, v3, v4}},
					},
				}

				for _, tc := range pathTestCases {
					t.Run(tc.name, func(t *testing.T) {
						path, err := g.FindPath(tc.fromKey, tc.toKey)
						require.NoError(t, err)
						require.Contains(t, tc.possiblePaths, path)
					})
				}
			})

			t.Run("returns ErrVertexNotFound creating an edge for non-existing vertices", func(t *testing.T) {
				g := NewGraph()
				err := g.AddVertex(v1)
				require.NoError(t, err)

				err = g.AddEdge(v1.Key, "non-existent")
				require.ErrorIs(t, err, ErrVertexNotFound)

				err = g.AddEdge("non-existent", v1.Key)
				require.ErrorIs(t, err, ErrVertexNotFound)
			})

			t.Run("returns ErrVertexNotFound when searching connections of a non-existing vertex", func(t *testing.T) {
				g := NewGraph()
				connected, err := g.FindConnected("non-existing")
				require.ErrorIs(t, err, ErrVertexNotFound)
				require.Nil(t, connected)
			})

			t.Run("returns ErrVertexNotFound finding a path for non-existing vertices", func(t *testing.T) {
				g := NewGraph()
				err := g.AddVertex(v1)
				require.NoError(t, err)

				path, err := g.FindPath(v1.Key, "non-existent")
				require.ErrorIs(t, err, ErrVertexNotFound)
				require.Nil(t, path)

				path, err = g.FindPath("non-existent", v1.Key)
				require.ErrorIs(t, err, ErrVertexNotFound)
				require.Nil(t, path)
			})

			t.Run("returns no deleted vertices when search connections and paths", func(t *testing.T) {
				// 1.
				// v1---->v3(X)-->v5
				//  |     ^
				//  v     |
				// v2     v4
				//
				// 2.
				// v1             v5
				//  |
				//  v
				// v2     v4
				//
				// v3 gets deleted after edges are set:
				//
				// v1 is connected to v2
				// v2 is not connected
				// v3 returns ErrVertexNotFound
				// v4 is not connected
				// v5 is not connected
				g := NewGraph()

				t.Run("adding vertices", func(t *testing.T) {
					err := g.AddVertex(v1)
					require.NoError(t, err)

					err = g.AddVertex(v2)
					require.NoError(t, err)

					err = g.AddVertex(v3)
					require.NoError(t, err)

					err = g.AddVertex(v4)
					require.NoError(t, err)

					err = g.AddVertex(v5)
					require.NoError(t, err)
				})

				t.Run("adding edges", func(t *testing.T) {
					err := g.AddEdge(v1.Key, v2.Key)
					require.NoError(t, err)

					err = g.AddEdge(v1.Key, v3.Key)
					require.NoError(t, err)

					err = g.AddEdge(v3.Key, v5.Key)
					require.NoError(t, err)

					err = g.AddEdge(v4.Key, v3.Key)
					require.NoError(t, err)
				})

				err := g.RemoveVertex(v3.Key)
				require.NoError(t, err)

				connectivityTestCases := []struct {
					name      string
					key       string
					connected []Vertex
				}{
					{
						name:      "v1 connects to v2",
						key:       v1.Key,
						connected: []Vertex{v2},
					},
					{
						name:      "v2 is not connected",
						key:       v2.Key,
						connected: []Vertex{},
					},
					{
						name:      "v4 is not connected",
						key:       v4.Key,
						connected: []Vertex{},
					},
					{
						name:      "v5 is not connected",
						key:       v5.Key,
						connected: []Vertex{},
					},
				}

				for _, tc := range connectivityTestCases {
					t.Run(tc.name, func(t *testing.T) {
						connected, err := g.FindConnected(tc.key)
						require.NoError(t, err)
						sortVertices(connected)
						sortVertices(tc.connected)
						require.Equal(t, tc.connected, connected)
					})
				}

				pathTestCases := []struct {
					name          string
					fromKey       string
					toKey         string
					possiblePaths [][]Vertex
					expectedErr   error
				}{
					{
						name:          "path from v1 to v2 still exists",
						fromKey:       v1.Key,
						toKey:         v2.Key,
						possiblePaths: [][]Vertex{{v1, v2}},
					},
					{
						name:        "path from v1 to v5 does not exist anymore",
						fromKey:     v1.Key,
						toKey:       v5.Key,
						expectedErr: ErrPathNotFound,
					},
					{
						name:        "path from v4 to v5 does not exist anymore",
						fromKey:     v4.Key,
						toKey:       v5.Key,
						expectedErr: ErrPathNotFound,
					},
				}
				for _, tc := range pathTestCases {
					t.Run(tc.name, func(t *testing.T) {
						path, err := g.FindPath(tc.fromKey, tc.toKey)
						if tc.expectedErr != nil {
							require.Error(t, err)
							require.ErrorIs(t, err, tc.expectedErr)
							require.Nil(t, path)
							return
						}
						require.NoError(t, err)
						require.Contains(t, tc.possiblePaths, path)
					})
				}
			})

			t.Run("disconnects vertices after edge removal", func(t *testing.T) {
				// 1.
				// v1--X-->v3-->v5
				//  |      ^
				//  v      |
				// v2      v4
				//
				// 2.
				// v1      v3-->v5
				//  |      ^
				//  v      |
				// v2      v4
				//
				// v3 gets deleted after edges are set:
				//
				// v1 is connected to v2
				// v2 is not connected
				// v3 is connected to v5
				// v4 is connected to v3 and v5
				// v5 is not connected
				g := NewGraph()

				// vertices
				err := g.AddVertex(v1)
				require.NoError(t, err)

				err = g.AddVertex(v2)
				require.NoError(t, err)

				err = g.AddVertex(v3)
				require.NoError(t, err)

				err = g.AddVertex(v4)
				require.NoError(t, err)

				err = g.AddVertex(v5)
				require.NoError(t, err)

				// edges
				err = g.AddEdge(v1.Key, v2.Key)
				require.NoError(t, err)

				err = g.AddEdge(v1.Key, v3.Key)
				require.NoError(t, err)

				err = g.AddEdge(v3.Key, v5.Key)
				require.NoError(t, err)

				err = g.AddEdge(v4.Key, v3.Key)
				require.NoError(t, err)

				err = g.RemoveEdge(v1.Key, v3.Key)
				require.NoError(t, err)

				connected, err := g.FindConnected(v1.Key)
				require.NoError(t, err)
				require.Equal(t, []Vertex{v2}, connected)

				connected, err = g.FindConnected(v2.Key)
				require.NoError(t, err)
				require.Equal(t, []Vertex{}, connected)

				connected, err = g.FindConnected(v3.Key)
				require.NoError(t, err)
				require.Equal(t, []Vertex{v5}, connected)

				connected, err = g.FindConnected(v4.Key)
				require.NoError(t, err)
				sortVertices(connected)
				require.Equal(t, []Vertex{v3, v5}, connected)

				connected, err = g.FindConnected(v5.Key)
				require.NoError(t, err)
				require.Equal(t, []Vertex{}, connected)
			})

			t.Run("returns ErrVertexNotFound when removing an edge of non-existing vertices", func(t *testing.T) {
				g := NewGraph()

				err := g.AddVertex(v1)
				require.NoError(t, err)

				err = g.AddVertex(v2)
				require.NoError(t, err)

				err = g.RemoveEdge(v1.Key, "non-existing")
				require.ErrorIs(t, err, ErrVertexNotFound)
				err = g.RemoveEdge("non-existing", v1.Key)
				require.ErrorIs(t, err, ErrVertexNotFound)

				// one of the vertices of the added edge has been removed
				err = g.AddEdge(v1.Key, v2.Key)
				require.NoError(t, err)

				err = g.RemoveVertex(v1.Key)
				require.NoError(t, err)

				err = g.RemoveEdge(v1.Key, v2.Key)
				require.ErrorIs(t, err, ErrVertexNotFound)
			})
		})
	})
}
