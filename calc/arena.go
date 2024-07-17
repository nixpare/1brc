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

func newRegion(size uintptr) *region {
	p := uintptr(mem.Malloc(size))
	if p == 0 {
		panic("arena: create new memory block failed")
	}

	return &region{
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
	single []*region
	multi []*region
	step uintptr
}

func NewArena(allocStep uintptr) *Arena {
	return &Arena{ step: allocStep }
}

func (a *Arena) Alloc(sizeof, alignof uintptr) unsafe.Pointer {
	for _, r := range a.single {
		ptr := r.allocate(1, sizeof, alignof)
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
	for _, r := range a.multi {
		ptr := r.allocate(n, sizeof, alignof)
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
	step, memsize := a.step, uintptr(2) * sizeof
	if step < memsize {
		step = memsize
	}

	a.single = append(a.single, newRegion(step))
}

func (a *Arena) allocateMultiRegion(n int, sizeof uintptr) {
	step, memsize := a.step, uintptr(n+1) * sizeof
	if step < memsize {
		step = memsize
	}

	a.multi = append(a.multi, newRegion(step))
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
