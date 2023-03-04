package cache

import (
	"container/list"
	"fmt"
)

type segmentedLRU struct {
	data                     map[uint64]*list.Element
	stageOneCap, stageTwoCap int
	stageOne, stageTwo       *list.List
}

const (
	STAGE_ONE = iota + 1
	STAGE_TWO
)

func newSLRU(data map[uint64]*list.Element, stageOneCap, stageTwoCap int) *segmentedLRU {
	return &segmentedLRU{
		data:        data,
		stageOneCap: stageOneCap,
		stageTwoCap: stageTwoCap,
		stageOne:    list.New(),
		stageTwo:    list.New(),
	}
}

func (slru *segmentedLRU) add(newitem storeItem) {
	newitem.stage = STAGE_ONE
	if slru.stageOne.Len() < slru.stageOneCap || slru.stageOne.Len() + slru.stageTwo.Len() < slru.stageOneCap + slru.stageTwoCap {
		slru.data[newitem.key] = slru.stageOne.PushFront(&newitem)
		return
	}

	backEle := slru.stageOne.Back()
	backVal := backEle.Value.(*storeItem)

	delete(slru.data, backVal.key)
	*backVal = newitem

	slru.data[newitem.key] = backEle
	slru.stageOne.MoveToFront(backEle)
}

func (slru *segmentedLRU) get(v *list.Element) {
	val := v.Value.(*storeItem)
	if val.stage == STAGE_TWO {
		slru.stageTwo.MoveToFront(v)
		return
	}

	if slru.stageTwo.Len() < slru.stageTwoCap {
		val.stage = STAGE_TWO
		slru.stageOne.Remove(v)
		slru.data[val.key] = slru.stageTwo.PushFront(val)
		return
	}

	twoBackEle := slru.stageTwo.Back()
	twoBackVal := twoBackEle.Value.(*storeItem)

	itemVal := v.Value.(*storeItem)

	v.Value, twoBackEle.Value = twoBackVal, itemVal

	slru.data[val.key] = twoBackEle
	slru.data[twoBackVal.key] = v

	slru.stageTwo.MoveToFront(twoBackEle)
	slru.stageOne.MoveToFront(v)

	val.stage = STAGE_TWO
	twoBackVal.stage = STAGE_ONE
}

func (slru *segmentedLRU) Len() int {
	return slru.stageOne.Len() + slru.stageTwo.Len()
}

func (slru *segmentedLRU) victim() *storeItem {
	if slru.Len() == 0 {
		return nil
	}

	oneBackEle := slru.stageOne.Back()
	return oneBackEle.Value.(*storeItem)
}

func (slru *segmentedLRU) String() string {
	var s string
	for e := slru.stageTwo.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	s += fmt.Sprintf(" | ")
	for e := slru.stageOne.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	return s
}
