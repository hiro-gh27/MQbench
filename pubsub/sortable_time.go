package pubsub

import (
	"sort"
	"time"
)

type SortableTime struct {
	inner    []time.Time
	isSorted bool
}

func NewSortableTime(times []time.Time) *SortableTime {
	st := &SortableTime{
	}
	copy(st.inner, times)
	return st
}

func (st *SortableTime) get() []time.Time {
	if !st.isSorted {
		sort.Sort(st)
	}
	return st.inner
}

func (st *SortableTime) Len() int {
	return len(st.inner)
}

func (st *SortableTime) Less(i, j int) bool {
	itime := st.inner[i]
	jtime := st.inner[j]
	duration := jtime.Sub(itime)
	return duration > 0
}

func (st *SortableTime) Swap(i, j int) {
	st.inner[i], st.inner[j] = st.inner[j], st.inner[i]
}
