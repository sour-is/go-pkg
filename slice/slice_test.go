package slice_test

import (
	"testing"

	"github.com/matryer/is"
	"go.sour.is/pkg/slice"
)

func TestAlign(t *testing.T) {
	type testCase struct {
		left, right []string
		combined    []slice.Pair[*string, *string]
	}

	tests := []testCase{
		{
			left:  []string{"1", "3", "5"},
			right: []string{"2", "3", "4"},
			combined: []slice.Pair[*string, *string]{
				{ptr("1"), nil},
				{nil, ptr("2")},
				{ptr("3"), ptr("3")},
				{nil, ptr("4")},
				{ptr("5"), nil},
			},
		},

		{
			left:  []string{"2", "3", "4"},
			right: []string{"1", "3", "5"},
			combined: []slice.Pair[*string, *string]{
				{nil, ptr("1")},
				{ptr("2"), nil},
				{ptr("3"), ptr("3")},
				{ptr("4"), nil},
				{nil, ptr("5")},
			},
		},
	}

	is := is.New(t)

	for _, tt := range tests {
		combined := slice.Align(tt.left, tt.right, func(l, r string) bool { return l < r })
		is.Equal(len(combined), len(tt.combined))
		for i := range combined {
			is.Equal(combined[i], tt.combined[i])
		}
	}
}

func ptr[T any](v T) *T { return &v }

func TestHeapSort(t *testing.T) {
	is := is.New(t)

	arr := []int{9, 4, 3, 8, 10, 2, 5}
	slice.HeapSort(arr, func(l, r int) bool { return l < r })

	is.Equal(arr, []int{2, 3, 4, 5, 8, 9, 10})
}

func TestBuildHeap(t *testing.T) {
	is := is.New(t)

	arr := []int{1, 3, 5, 4, 6, 13, 10, 9, 8, 15, 17}
	slice.BuildMaxHeap(arr, func(l, r int) bool { return l < r })

	is.Equal(arr, []int{17, 15, 13, 9, 6, 5, 10, 4, 8, 3, 1})
}
