package main

import (
	"sync"
	"unsafe"

	"github.com/nixpare/mem"
)

type region struct {
	start unsafe.Pointer
	end   uintptr
	next  uintptr
}

func newRegion(size uintptr) region {
	p := mem.Malloc(size)
	ptr := uintptr(p)
	if ptr == 0 {
		panic("arena: create new memory block failed")
	}

	return region{
		start: p,
		end:   ptr + size,
		next:  ptr,
	}
}

func (r *region) allocate(n int, sizeof uintptr, alignof uintptr) uintptr {
	aligned := (r.next + (alignof - 1)) & ^(alignof - 1)

	newNext := aligned + uintptr(n)*sizeof
	if newNext > r.end {
		return 0
	}

	r.next = newNext
	return aligned
}

func (r region) free() {
	mem.Free(r.start)
}

type Arena struct {
	regions   []region
	allocSize uintptr
	m         sync.Mutex
}

func NewArena(allocSize uintptr) *Arena {
	return &Arena{allocSize: allocSize}
}

func (a *Arena) Alloc(sizeof, alignof uintptr) unsafe.Pointer {
	return a.AllocN(1, sizeof, alignof)
}

func (a *Arena) AllocN(n int, sizeof, alignof uintptr) unsafe.Pointer {
	a.m.Lock()
	defer a.m.Unlock()

	for i := range a.regions {
		ptr := a.regions[i].allocate(n, sizeof, alignof)
		if ptr != 0 {
			return unsafe.Pointer(ptr)
		}
	}

	a.allocRegion(n, sizeof)

	ptr := a.regions[len(a.regions)-1].allocate(n, sizeof, alignof)
	if ptr == 0 {
		panic("arena: alloc failed with new memory block")
	}

	return unsafe.Pointer(ptr)
}

func (a *Arena) Regions() int {
	return len(a.regions)
}

func (a *Arena) allocRegion(n int, sizeof uintptr) {
	memSize, reqSize := a.allocSize, uintptr(n)*sizeof
	if memSize < reqSize {
		memSize = reqSize
	}

	a.regions = append(a.regions, newRegion(memSize))
}

func (a *Arena) Free() {
	a.m.Lock()
	defer a.m.Unlock()

	for _, r := range a.regions {
		r.free()
	}
	a.regions = nil
}
