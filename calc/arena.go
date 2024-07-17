package main

import (
	"unsafe"

	"github.com/nixpare/mem"
)

type region struct {
	start uintptr
	end uintptr
	next uintptr
}

func newRegion(size uintptr) region {
	p := uintptr(mem.Malloc(size))
	if p == 0 {
		panic("arena: create new memory block failed")
	}

	return region{
		start: p,
		end: p + size,
		next: p,
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

	newNext := aligned + uintptr(n) * sizeof
	if newNext > r.end {
		return 0
	}

	r.next = newNext
	return aligned
}

func (r region) free() {
	mem.Free(unsafe.Pointer(r.start))
}

type Arena struct {
	single []region
	multi []region
	singleAlloc uintptr
	multiAlloc uintptr
}

func NewArena(singleAlloc, multiAlloc uintptr) *Arena {
	return &Arena{ singleAlloc: singleAlloc, multiAlloc: multiAlloc }
}

func (a *Arena) Alloc(sizeof, alignof uintptr) unsafe.Pointer {
	for i := range a.single {
		ptr := a.single[i].allocate(1, sizeof, alignof)
		if ptr != 0 {
			return unsafe.Pointer(ptr)
		}
	}

	a.allocateSingleRegion(sizeof)
	
	ptr := a.single[len(a.single)-1].allocate(1, sizeof, alignof)
	if ptr == 0 {
		panic("arena: alloc failed with new memory block")
	}

	return unsafe.Pointer(ptr)
}

func (a *Arena) AllocN(n int, sizeof, alignof uintptr) unsafe.Pointer {
	for i := range a.multi {
		ptr := a.multi[i].allocate(n, sizeof, alignof)
		if ptr != 0 {
			return unsafe.Pointer(ptr)
		}
	}

	a.allocateMultiRegion(n, sizeof)
	
	ptr := a.multi[len(a.multi)-1].allocate(n, sizeof, alignof)
	if ptr == 0 {
		panic("arena: alloc failed with new memory block")
	}

	return unsafe.Pointer(ptr)
}

func (a *Arena) allocateSingleRegion(sizeof uintptr) {
	memSize, reqSize := a.singleAlloc, sizeof
	if memSize < reqSize {
		memSize = reqSize
	}

	a.single = append(a.single, newRegion(memSize))
}

func (a *Arena) allocateMultiRegion(n int, sizeof uintptr) {
	memSize, reqSize := a.multiAlloc, uintptr(n) * sizeof
	if memSize < reqSize {
		memSize = reqSize
	}

	a.multi = append(a.multi, newRegion(memSize))
}

func (a *Arena) Free() {
	for _, r := range a.single {
		r.free()
	}
	a.single = nil
	
	for _, r := range a.multi {
		r.free()
	}
	a.multi = nil
}
