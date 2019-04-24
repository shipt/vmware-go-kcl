package custom

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/vmware/vmware-go-kcl/clientlibrary/checkpoint"
	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
	"github.com/vmware/vmware-go-kcl/clientlibrary/partition"
)

type redisCheckpoint struct {
	ShardID       string
	AssignedTo    string
	LeaseTimeout  time.Time
	Checkpoint    string
	ParentShardID string
}

type RedisCheckpointer struct {
	LeaseDuration int
	kclConfig     *config.KinesisClientLibConfiguration
	Retries       int

	pool     *redis.Pool
	redisURL string
}

const (
	SHARD_LEASE_HASH_KEY = "shard_leases"
)

func NewRedisCheckpointer(redisURL string, kclConfig *config.KinesisClientLibConfiguration) RedisCheckpointer {
	checkpointer := RedisCheckpointer{
		// LeaseDuration: kclConfig.FailoverTimeMillis,
		kclConfig: kclConfig,
		Retries:   5,
		redisURL:  redisURL,
	}

	return checkpointer
}

func (c *RedisCheckpointer) Init() error {
	connection := func() (redis.Conn, error) {
		return redis.DialURL(c.redisURL)
	}

	c.pool = &redis.Pool{Dial: connection, MaxIdle: 10}
	return nil
}

func (c *RedisCheckpointer) GetLease(shard *partition.ShardStatus, newAssignTo string) error {
	newLeaseTimeout := time.Now().Add(time.Duration(c.LeaseDuration) * time.Millisecond).UTC()
	chk, err := c.getItem(shard.ID)
	if err != nil {
		return err
	}

	// assignedVar := chk.assignedTo
	// leaseVar := chk.leaseTimeout
	if !time.Now().UTC().After(chk.LeaseTimeout) && chk.AssignedTo != newAssignTo {
		return errors.New(checkpoint.ErrLeaseNotAquired)
	}

	//TODO: replicate conditional update in lua script

	rChk := redisCheckpoint{
		ShardID:      shard.ID,
		AssignedTo:   newAssignTo,
		LeaseTimeout: newLeaseTimeout,
	}

	if shard.ParentShardId != "" {
		rChk.ParentShardID = shard.ParentShardId
	}

	if shard.Checkpoint != "" {
		rChk.Checkpoint = shard.Checkpoint
	}

	if err := c.setItem(shard.ID, rChk); err != nil {
		return err
	}

	shard.Mux.Lock()
	shard.AssignedTo = newAssignTo
	shard.LeaseTimeout = newLeaseTimeout
	shard.Mux.Unlock()

	return nil
}

func (c *RedisCheckpointer) RemoveLeaseInfo(shardID string) error {
	con := c.pool.Get()
	if err := con.Err(); err != nil {
		return err
	}
	defer con.Close()

	if err := con.Send("HDEL", SHARD_LEASE_HASH_KEY, shardID); err != nil {
		return err
	}

	return nil
}

func (c *RedisCheckpointer) CheckpointSequence(shard *partition.ShardStatus) error {
	leaseTimeout := shard.LeaseTimeout.UTC()
	s := redisCheckpoint{
		ShardID:      shard.ID,
		LeaseTimeout: leaseTimeout,
		AssignedTo:   shard.AssignedTo,
		Checkpoint:   shard.Checkpoint,
	}

	return c.setItem(s.ShardID, s)
}

func (c *RedisCheckpointer) FetchCheckpoint(shard *partition.ShardStatus) error {
	chk, err := c.getItem(shard.ID)
	if err != nil {
		return err
	}

	if chk.ShardID == "" {
		return checkpoint.ErrSequenceIDNotFound
	}

	shard.Mux.Lock()
	defer shard.Mux.Unlock()

	shard.Checkpoint = chk.Checkpoint
	if chk.AssignedTo != "" {
		shard.AssignedTo = chk.AssignedTo
	}

	return nil
}

func (c *RedisCheckpointer) setItem(key string, rCheckpoint redisCheckpoint) error {
	blob, err := json.Marshal(rCheckpoint)
	if err != nil {
		return err
	}

	con := c.pool.Get()
	if err := con.Err(); err != nil {
		return err
	}
	defer con.Close()

	err = con.Send("HSET", SHARD_LEASE_HASH_KEY, key, blob)
	if err != nil {
		return err
	}

	return nil
}

func (c *RedisCheckpointer) getItem(shardID string) (redisCheckpoint, error) {
	var checkpoint redisCheckpoint
	con := c.pool.Get()
	if err := con.Err(); err != nil {
		return checkpoint, err
	}

	defer con.Close()

	r, cmdErr := con.Do("HGET", SHARD_LEASE_HASH_KEY, shardID)
	blob, rErr := redis.Bytes(r, cmdErr)

	if cmdErr != nil {
		return checkpoint, cmdErr
	}

	// not found
	if rErr != nil {
		return checkpoint, nil
	}

	if err := json.Unmarshal(blob, &checkpoint); err != nil {
		return checkpoint, err
	}
	return checkpoint, nil
}
