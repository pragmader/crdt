package lww

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	t.Run("CRDT properties", func(t *testing.T) {
		e1 := IDElement("element1")
		e2 := IDElement("element2")
		e3 := IDElement("element3")

		t.Run("Eventual convergence", func(t *testing.T) {
			// copies of shared objects are identical at all sites if updates
			// cease and all generated updates are propagated to all sites.

			t.Run("all actors converge to the same state after replication", func(t *testing.T) {
				// Time ->
				// A--Add(e1)-------------------\---|
				// B------Add(e2)----------------\--|=> A,B,C = {e1,e2,e3}
				// C-----------Add(e1), Add(e3)---\-|

				A := NewSet()
				B := NewSet()
				C := NewSet()

				A.Add(e1)

				B.Add(e2)

				C.Add(e1)
				C.Add(e3)

				replicateSets(A, B, C)

				a := A.List()
				b := B.List()
				c := C.List()

				sortElements(a)
				sortElements(b)
				sortElements(c)

				require.Equal(t, a, b)
				require.Equal(t, b, c)
			})
		})

		t.Run("Intention-preservation", func(t *testing.T) {
			// for any update O [A.Remove(e1)], the effect of executing O at all sites is the same
			// as the intention of O when executed at the site that originated it,
			// and the effect of executing O does not change the effect of non concurrent operations.

			t.Run("element removal gets replicated", func(t *testing.T) {
				// A--Add(e1)---------Remove(e1)--\---|
				// B----------Add(e1)--------------\--|=> A,B = {}

				A := NewSet()
				B := NewSet()

				A.Add(e1)
				B.Add(e1)
				A.Remove(e1.GetKey())

				replicateSets(A, B)

				found, err := A.Lookup(e1.GetKey())
				require.ErrorIs(t, err, ErrElementNotFound)
				require.Nil(t, found)

				found, err = B.Lookup(e1.GetKey())
				require.ErrorIs(t, err, ErrElementNotFound)
				require.Nil(t, found)

				expected := []Element{}
				require.Equal(t, expected, A.List())
				require.Equal(t, expected, B.List())
			})
		})

		t.Run("Precedence", func(t *testing.T) {
			// if one update Oa [A.Remove(e1)] causally precedes another update Ob [B.Add(e1)],
			// then, at each site, the execution of Oa happens before the execution of Ob.

			t.Run("same element re-added after removal", func(t *testing.T) {
				// Time ->
				// A--Add(e1)--Remove(e1)---\---|
				// B---------------Add(e1)---\--|=> A,B = {e1}

				A := NewSet()
				B := NewSet()

				A.Add(e1)
				A.Remove(e1.GetKey())

				B.Add(e1)

				replicateSets(A, B)

				found, err := A.Lookup(e1.GetKey())
				require.NoError(t, err)
				require.Equal(t, e1, found)
				found, err = B.Lookup(e1.GetKey())
				require.NoError(t, err)
				require.Equal(t, e1, found)

				expected := []Element{e1}
				require.Equal(t, expected, A.List())
				require.Equal(t, expected, B.List())
			})
		})
	})

	t.Run("Set operations", func(t *testing.T) {
		key := "unqiue"
		element := IDElement(key)

		t.Run("Add/Lookup", func(t *testing.T) {
			t.Run("added element can be retrieved", func(t *testing.T) {
				s := NewSet()
				s.Add(element)

				retreived, err := s.Lookup(key)
				require.NoError(t, err)
				require.Equal(t, element, retreived)
			})

			t.Run("adding the same element twice does not panic", func(t *testing.T) {
				s := NewSet()

				require.NotPanics(t, func() {
					s.Add(element)
					s.Add(element)
				})
			})

			t.Run("retrieving a non-existing element returns ErrElementNotFound", func(t *testing.T) {
				s := NewSet()

				element, err := s.Lookup("non-existing")
				require.ErrorIs(t, err, ErrElementNotFound)
				require.Nil(t, element)
			})
		})

		t.Run("Remove", func(t *testing.T) {
			t.Run("removes an existing element", func(t *testing.T) {
				s := NewSet()
				s.Add(element)
				s.Remove(key)

				element, err := s.Lookup(key)
				require.ErrorIs(t, err, ErrElementNotFound)
				require.Nil(t, element)
			})

			t.Run("does not panic for non-existing element", func(t *testing.T) {
				s := NewSet()
				require.NotPanics(t, func() {
					s.Remove("non-existing")
				})
			})
		})
	})
}
