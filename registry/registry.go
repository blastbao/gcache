package registry

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"
)

const (
	// 续约间隔，单位秒
	keepAliveTTL = 10
	// 事件通道缓冲区大小
	eventChanSize = 10
)

// Event 服务变化事件
type Event struct {
	AddAddr    string
	DeleteAddr string
}

// Registry 名字服务
type Registry struct {
	endpoints []string // etcd服务器地址
	mu        sync.Mutex
	client    *etcd.Client
	prefix    string // etcd名字服务key前缀
}

func New(prefix string, endpoints []string) (*Registry, error) {
	client, err := etcd.New(etcd.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &Registry{
		endpoints: endpoints,
		client:    client,
		prefix:    prefix,
	}, nil
}

// Register 注册服务
//
// etcd 的租约（Lease）机制是用于管理键值对（KV）在指定时间内的生存周期。
// 租约机制使得存储在 etcd 中的键值对能够在一定时间后自动过期，或者在租约保持活跃的情况下，持续保持其有效性。
// 它是分布式系统中实现服务发现、心跳机制和自动故障检测的重要工具。
//
// 租约机制实现服务注册的自动失效，如果服务崩溃或退出，etcd 会在租约过期后自动删除服务的注册信息，避免服务长时间失联导致注册信息一直保留在 etcd 中。
func (r *Registry) Register(ctx context.Context, addr string) error {
	kv := etcd.NewKV(r.client)
	lease := etcd.NewLease(r.client)             // 创建租约
	grant, err := lease.Grant(ctx, keepAliveTTL) // 设置租约过期时间(TTL)为 10 秒
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s%s", r.prefix, addr)
	if _, err := kv.Put(ctx, key, addr, etcd.WithLease(grant.ID)); err != nil { // 将 kv 与租约关联
		return err
	}
	ch, err := lease.KeepAlive(ctx, grant.ID) // 启动 KeepAlive 机制，保持租约活跃
	if err != nil {
		return err
	}
	go func() {
		for {
			_, ok := <-ch
			if !ok {
				fmt.Println("Lease expired!")
				return
			}
			log.Println("Lease renewed successfully")
		}
	}()
	return nil
}

// GetAddrs 获取节点地址列表
func (r *Registry) GetAddrs(ctx context.Context) ([]string, error) {
	kv := etcd.NewKV(r.client)
	resp, err := kv.Get(ctx, r.prefix, etcd.WithPrefix())
	if err != nil {
		return nil, err
	}
	addrs := make([]string, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		addrs[i] = string(kv.Value)
	}
	return addrs, nil
}

// Watch 发现服务
func (r *Registry) Watch(ctx context.Context) <-chan Event {
	watcher := etcd.NewWatcher(r.client)
	watchChan := watcher.Watch(ctx, r.prefix, etcd.WithPrefix())
	ch := make(chan Event, eventChanSize)
	go func() {
		for watchRsp := range watchChan {
			for _, event := range watchRsp.Events {
				switch event.Type {
				case mvccpb.PUT:
					ch <- Event{AddAddr: string(event.Kv.Value)}
				case mvccpb.DELETE:
					ch <- Event{DeleteAddr: string(event.Kv.Key[len(r.prefix):])}
				}
			}
		}
		close(ch)
	}()
	return ch
}
