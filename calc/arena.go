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
	current := r.next
	offset := current % sizeof
	var adjustment uintptr
	if offset != 0 {
		adjustment = alignof - offset
	}
	aligned := current + adjustment

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

type arena struct {
	regions   []region
	allocSize uintptr
	m         sync.Mutex
}

func newArena(allocSize uintptr) *arena {
	return &arena{allocSize: allocSize}
}

func (a *arena) Alloc(sizeof, alignof uintptr) unsafe.Pointer {
	return a.AllocN(1, sizeof, alignof)
}

func (a *arena) AllocN(n int, sizeof, alignof uintptr) unsafe.Pointer {
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

func (a *arena) Regions() int {
	return len(a.regions)
}

func (a *arena) allocRegion(n int, sizeof uintptr) {
	memSize, reqSize := a.allocSize, uintptr(n)*sizeof
	if memSize < reqSize {
		memSize = reqSize
	}

	a.regions = append(a.regions, newRegion(memSize))
}

func (a *arena) Free() {
	a.m.Lock()
	defer a.m.Unlock()

	for _, r := range a.regions {
		r.free()
	}
	a.regions = nil
}

type Arena struct {
	objectA *arena
	sliceA  *arena
	stringA *arena
}

func NewArena(objAlloc, sliceAlloc, strAlloc uintptr) *Arena {
	return &Arena{
		objectA: newArena(objAlloc),
		sliceA:  newArena(sliceAlloc),
		stringA: newArena(strAlloc),
	}
}

func (a *Arena) Alloc(sizeof, alignof uintptr) unsafe.Pointer {
	return a.objectA.Alloc(sizeof, alignof)
}

func (a *Arena) AllocSlice(n int, sizeof, alignof uintptr) unsafe.Pointer {
	return a.sliceA.AllocN(n, sizeof, alignof)
}

func (a *Arena) AllocString(n int, sizeof, alignof uintptr) unsafe.Pointer {
	return a.stringA.AllocN(n, sizeof, alignof)
}

func (a *Arena) Regions() int {
	return a.objectA.Regions() +
		a.sliceA.Regions() + a.stringA.Regions()
}

func (a *Arena) Free() {
	a.objectA.Free()
	a.sliceA.Free()
	a.stringA.Free()
}
