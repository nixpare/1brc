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
	r []*region
	step uintptr
}

func NewArena(allocStep uintptr) *Arena {
	return &Arena{ step: allocStep }
}

func (a *Arena) Alloc(sizeof, alignof uintptr) unsafe.Pointer {
	return a.AllocN(1, sizeof, alignof)
}

func (a *Arena) AllocN(n int, sizeof, alignof uintptr) unsafe.Pointer {
	if len(a.r) == 0 {
		a.allocateRegion(n, sizeof)

		ptr := a.r[0].allocate(n, sizeof, alignof)
		if ptr == 0 {
			panic("arena: alloc failed with new memory block")
		}
	}

	ptr := a.r[len(a.r)-1].allocate(n, sizeof, alignof)
	if ptr == 0 {
		a.r = append(a.r, newRegion(a.step))
		ptr = a.r[len(a.r)-1].allocate(n, sizeof, alignof)
		if ptr == 0 {
			panic("arena: alloc failed with new memory block")
		}
	}

	return unsafe.Pointer(ptr)
}

func (a *Arena) allocateRegion(n int, sizeof uintptr) {
	step, size := a.step, uintptr(n+1) * sizeof
	if step >= size {
		a.r = append(a.r, newRegion(a.step))
	} else {
		a.r = append(a.r, newRegion(size))
	}
}

func (a *Arena) Free() {
	for _, r := range a.r {
		r.free()
	}
	a.r = nil
}
