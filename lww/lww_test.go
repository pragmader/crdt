package lww

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func sortElements(list []Element) {
	sort.Slice(list, func(i, j int) bool {
		return list[i].GetKey() < list[j].GetKey()
	})
}

func sortVertices(list []Vertex) {
	sort.Slice(list, func(i, j int) bool {
		return list[i].GetKey() < list[j].GetKey()
	})
}

func replicateSets(sets ...Set) {
	for _, to := range sets {
		for _, from := range sets {
			if from.mutex == to.mutex {
				continue
			}
			to.Merge(from)
		}
	}
}

func replicateGraphs(graphs ...Graph) {
	for _, to := range graphs {
		for _, from := range graphs {
			if from.mutex == to.mutex {
				continue
			}
			to.Merge(from)
		}
	}
}

func equalGraphs(t *testing.T, graphs ...Graph) {
	require.True(t, len(graphs) > 1, "2 and more graphs can be compared")
	g1 := graphs[0]
	g1List, err := g1.List()
	require.NoError(t, err)

	graphs = graphs[1:]
	for _, g2 := range graphs {
		g2List, err := g2.List()
		require.NoError(t, err)
		require.Equal(t, g1List, g2List)
	}
}
