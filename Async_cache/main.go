package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// первая реализация с ttl
type Cache struct {
	m   map[string]elem
	mu  *sync.RWMutex
	ttl time.Duration
}
type elem struct {
	value string
	t     time.Time
}

func newCache(ctx context.Context, t, gcTime time.Duration, capacity int) (*Cache, error) {
	if t < 0 {
		return nil, fmt.Errorf("invalid ttl")
	}
	if capacity < 0 {
		return nil, fmt.Errorf("invalid capacity")
	}
	if gcTime <= 0 {
		return nil, fmt.Errorf("invalid gc duration")
	}
	c := &Cache{
		m:   make(map[string]elem, capacity),
		mu:  &sync.RWMutex{},
		ttl: t,
	}
	go c.gc(ctx, gcTime)
	return c, nil
}

type message struct {
	v   elem
	err error
}

func (c *Cache) Get(ctx context.Context, key string) (string, error) {
	ch := make(chan message, 1)
	go func() {
		c.mu.RLock()
		defer c.mu.RUnlock()
		if v, ok := c.m[key]; !ok {
			ch <- message{v, fmt.Errorf("no exist value in cache")}
		} else {
			ch <- message{v, nil}
		}
	}()
	select {
	case <-ctx.Done():
		return "", fmt.Errorf("ctx done%w", ctx.Err())
	case res := <-ch:
		if res.err != nil {
			return "", res.err
		} else {
			if res.v.t.Add(c.ttl).After(time.Now()) || res.v.t.Add(c.ttl).Equal(time.Now()) {
				return res.v.value, nil
			}
			return "", fmt.Errorf("elem is not actaulity")
		}
	}
}
func (c *Cache) Set(ctx context.Context, key string, value string) error {
	ch := make(chan struct{}, 1)
	go func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		default:
			c.m[key] = elem{value: value, t: time.Now()}
		}
		close(ch)
	}()
	select {
	case <-ctx.Done():
		return fmt.Errorf("ctx done %w", ctx.Err())
	case <-ch:
		return nil
	}
}
func (c *Cache) gc(ctx context.Context, t time.Duration) {
	ticker := time.NewTicker(t)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			for k, v := range c.m {
				if v.t.Add(c.ttl).Before(time.Now()) {
					delete(c.m, k)
				}
			}
			c.mu.Unlock()
		}
	}
}

// LRU cache за основу возьму задачу на leetcode и адаптирую под многопоточность
type LRUCache struct {
	m    map[int]*Node
	Tail *Node
	Head *Node
	cap  int
	now  int
	mu   *sync.RWMutex
}
type Node struct {
	Next *Node
	Prev *Node
	Val  int
	Key  int
}

func Constructor(capacity int) LRUCache {
	if capacity < 0 {
		panic("LRU cache capacity must be non-negative")
	}
	head := &Node{}
	tail := &Node{}
	head.Next = tail
	tail.Prev = head
	return LRUCache{
		make(map[int]*Node, capacity),
		tail,
		head,
		capacity,
		0,
		&sync.RWMutex{},
	}
}

func (this *LRUCache) Get(ctx context.Context, key int) (int, error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	type result struct {
		value int
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		this.mu.Lock()
		defer this.mu.Unlock()
		if err := ctx.Err(); err != nil {
			ch <- result{err: err}
			return
		}
		v, ok := this.m[key]
		if !ok {
			ch <- result{value: -1, err: fmt.Errorf("value not found")}
			return
		}
		this.insertInB(v)
		ch <- result{value: v.Val}
	}()
	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	case res := <-ch:
		if res.err != nil {
			return -1, res.err
		}
		return res.value, nil
	}
}

func (this *LRUCache) Put(ctx context.Context, key int, value int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ch := make(chan error, 1)
	go func() {
		this.mu.Lock()
		defer this.mu.Unlock()
		if err := ctx.Err(); err != nil {
			ch <- err
			return
		}
		if this.cap == 0 {
			ch <- nil
			return
		}
		if v, ok := this.m[key]; ok {
			v.Val = value
			this.insertInB(v)
		} else {
			node := &Node{Key: key, Val: value}
			this.m[key] = node
			this.insertInB(node)
			this.now++
			if this.now > this.cap {
				this.del(this.Tail.Prev)
			}
		}
		ch <- nil
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ch:
		return err
	}
}

func (this *LRUCache) insertInB(node *Node) {
	if node.Prev != nil {
		node.Prev.Next = node.Next
		node.Next.Prev = node.Prev
	}
	first := this.Head.Next
	first.Prev = node
	node.Next = first
	node.Prev = this.Head
	this.Head.Next = node
}
func (this *LRUCache) del(node *Node) {
	node.Next.Prev = node.Prev
	node.Prev.Next = node.Next
	delete(this.m, node.Key)
	node.Prev = nil
	node.Next = nil
	this.now--
}
