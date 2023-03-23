package tarantool_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/connection_pool"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

func TestPrepareExecuteBlackbox(t *testing.T) {
	ttShutdown, _, err := setTarantoolCluster("3301", "3302", "3303")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	db, err := connection_pool.ConnectWithWritableAndRetryableDefaults(ctx, cancel, true, connection_pool.BasicAuth{"admin", "pass"},
		"localhost:3301",
		"localhost:3302",
		"localhost:3303",
		"bogus:2322",
	)
	defer func(db tarantool.Connector) {
		if db != nil {
			if cErr := db.Close(); cErr != nil {
				panic(cErr)
			}
			if db.ConnectedNow() {
				panic("still connected")
			}
		}
		ttShutdown()
		require.NoError(t, os.RemoveAll("/tmp/tarantool_data/"))
	}(db)
	if err != nil {
		panic(fmt.Sprintf("Could not connect to cluster %v", err))
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			execPrepareExecute(ctx, db)
		}()
	}
	wg.Wait()
}

func TestGracefulShutdownOnClose(t *testing.T) {
	ttShutdown, _, err := setTarantoolCluster("3301", "3302", "3303")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	db, err := connection_pool.ConnectWithWritableAndRetryableDefaults(ctx, cancel, true, connection_pool.BasicAuth{"admin", "pass"},
		"localhost:3301",
		"localhost:3302",
		"localhost:3303",
		"bogus:2322",
	)
	defer func(db tarantool.Connector) {
		if db != nil {
			if cErr := db.Close(); cErr != nil {
				panic(cErr)
			}
			if db.ConnectedNow() {
				panic("still connected")
			}
		}
		ttShutdown()
		require.NoError(t, os.RemoveAll("/tmp/tarantool_data/"))
	}(db)
	if err != nil {
		panic(fmt.Sprintf("Could not connect to cluster %v", err))
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			execPrepareExecute(ctx, db)
		}()
	}
	timerCloseWritable := time.NewTicker(3500 * time.Millisecond)
	defer timerCloseWritable.Stop()
	go func() {
		time.Sleep(time.Second)
		for {
			select {
			case _, open := <-timerCloseWritable.C:
				if !open || ctx.Err() != nil {
					return
				}
				c, err := db.(*connection_pool.RWBalancedConnectorAdapter).Pool().(*connection_pool.RetryablePool).GetConnByMode(connection_pool.RW)
				if c != nil && err == nil {
					if cErr := c.Close(); cErr != nil {
						log.Println(cErr)
					}
				}
			}
		}
	}()
	timerCloseNonWritable := time.NewTicker(2800 * time.Millisecond)
	defer timerCloseNonWritable.Stop()
	go func() {
		time.Sleep(time.Second)
		for {
			select {
			case _, open := <-timerCloseNonWritable.C:
				if !open || ctx.Err() != nil {
					return
				}
				c, err := db.(*connection_pool.RWBalancedConnectorAdapter).Pool().(*connection_pool.RetryablePool).GetConnByMode(connection_pool.RO)
				if c != nil && err == nil {
					if cErr := c.Close(); cErr != nil {
						log.Println(cErr)
					}
				}
			}
		}
	}()
	<-ctx.Done()
}

func execPrepareExecute(ctx context.Context, db tarantool.Connector) {
	execPrepareExecuteWithCallback(ctx, db, func() {}, func() {})
}
func execPrepareExecuteWithCallback(ctx context.Context, db tarantool.Connector, afterWrite, afterRead func()) {
	for ctx != nil && ctx.Err() == nil {
		id := uuid.New().String()
		id2 := uuid.New().String()
		if db == nil || ctx == nil {
			return
		}
		if r, rErr := db.PrepareExecute("INSERT into test_table(id, name, type) values (:id,:name,:type)", map[string]interface{}{"name": id2, "type": 3, "id": id}); (rErr != nil && !errors.Is(rErr, ctx.Err())) || (r != nil && r.Code != tarantool.OkCode) {
			if r == nil || r.Code != tarantool.ER_TUPLE_FOUND {
				panic(errors.New(fmt.Sprintf("Insert failed because: %v --- %v\n", rErr, r)))
			}
		}
		if db == nil || ctx == nil {
			return
		}
		afterWrite()
		if r, rErr := db.PrepareExecute("SELECT * from test_table where name=:name and type=:type", map[string]interface{}{"name": id2, "type": 3}); (rErr != nil && !errors.Is(rErr, ctx.Err())) || (r != nil && r.Code != tarantool.OkCode) {
			//if r, rErr := db.Select("TEST_TABLE", "T_IDX_1", 0, 1, tarantool.IterEq, []interface{}{id2, 3}); (rErr != nil && !errors.Is(rErr, ctx.Err())) || r.Code != tarantool.OkCode {
			panic(errors.New(fmt.Sprintf("Query failed because: %v------%v\n", rErr, r)))
		} else {
			if rErr != nil && !errors.Is(rErr, ctx.Err()) {
				panic(rErr)
			}
			if r != nil && len(r.Tuples()) != 0 {
				single := r.Tuples()[0]
				if single[0].(string) != id {
					panic(errors.New(fmt.Sprintf("expected:%v actual:%v", id, single[0].(string))))
				}
			} else {
				fmt.Sprintln("Query returned nothing")
			}
		}
		if db == nil || ctx == nil {
			return
		}
		afterRead()
		if r, rErr := db.PrepareExecute("DELETE from test_table where name=:name and type=:type", map[string]interface{}{"name": id2, "type": 3}); (rErr != nil && !errors.Is(rErr, ctx.Err())) || (r != nil && r.Code != tarantool.OkCode) {
			//if r, rErr := db.Delete("TEST_TABLE", "T_IDX_1", []interface{}{id2, 3}); (rErr != nil && !errors.Is(rErr, ctx.Err())) || r.Code != tarantool.OkCode {
			if r == nil || r.Code != tarantool.ER_TUPLE_NOT_FOUND {
				panic(errors.New(fmt.Sprintf("Delete failed because: %v --- %v\n", rErr, r)))
			}
		}
	}

}

func TestPrepareExecuteConnectionReestablished(t *testing.T) {
	ttShutdown, _, err := setTarantoolCluster("3301", "3302", "3303")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	db, err := connection_pool.ConnectWithWritableAndRetryableDefaults(ctx, cancel, true, connection_pool.BasicAuth{"admin", "pass"},
		"localhost:3301",
		"localhost:3302",
		"localhost:3303",
		"bogus:2322",
	)
	defer func(db tarantool.Connector) {
		if db != nil {
			if cErr := db.Close(); cErr != nil {
				panic(cErr)
			}
			if db.ConnectedNow() {
				panic("still connected")
			}
		}
		ttShutdown()
		require.NoError(t, os.RemoveAll("/tmp/tarantool_data/"))
	}(db)
	if err != nil {
		panic(fmt.Sprintf("Could not connect to cluster %v", err))
	}
	execPrepareExecute(ctx, db)
	ttShutdown()
	ttShutdown, _, err = setTarantoolCluster("3301", "3302", "3303")
	execPrepareExecute(ctx, db)
}

func TestMonitorConnections(t *testing.T) {
	ttShutdown, ttInstances, err := setTarantoolCluster("3301", "3302", "3303")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	db, err := connection_pool.ConnectWithWritableAndRetryableDefaults(ctx, cancel, true, connection_pool.BasicAuth{"admin", "pass"},
		"localhost:3301",
		"localhost:3302",
		"localhost:3303",
		"bogus:2322",
	)
	var shutdown3301 func()
	defer func(db tarantool.Connector, shutdowns []func()) {
		if db != nil {
			if cErr := db.Close(); cErr != nil {
				panic(cErr)
			}
			if db.ConnectedNow() {
				panic("still connected")
			}
		}
		for _, f := range shutdowns {
			if f != nil {
				f()
			}

		}
		require.NoError(t, os.RemoveAll("/tmp/tarantool_data/"))
	}(db, []func(){ttShutdown, shutdown3301})
	if err != nil {
		panic(fmt.Sprintf("Could not connect to cluster %v", err))
	}
	var newInst []test_helpers.TarantoolInstance
	execPrepareExecuteWithCallback(ctx, db, func() {
		// disable write-node before SELECTs, they should be fine, cuz go to RO instances
		test_helpers.StopTarantool(ttInstances[0])
		time.Sleep(2 * time.Second)
	}, func() {
		// re-enable it before DELETEs  - it should be added to pool and delete will not fail
		shutdown3301, newInst, err = setTarantoolCluster("3301")
		if err != nil {
			panic(err)
		}
		ttInstances[0] = newInst[0]
		time.Sleep(2 * time.Second)
	})
}

func setTarantoolCluster(ports ...string) (func(), []test_helpers.TarantoolInstance, error) {
	tts := make([]test_helpers.TarantoolInstance, len(ports))
	var wg sync.WaitGroup
	var errChan chan error
	errChan = make(chan error, len(ports))
	for i, p := range ports {
		wg.Add(1)
		go func(i int, port string) {
			defer wg.Done()
			var err error
			_ = os.MkdirAll(fmt.Sprintf("/tmp/tarantool_data/%v/memtx", port), 0777)
			_ = os.MkdirAll(fmt.Sprintf("/tmp/tarantool_data/%v/wal", port), 0777)
			tts[i], err = test_helpers.StartTarantool(test_helpers.StartOpts{
				InitScript:   fmt.Sprintf("./.testdata/tarantool_bootstrap_%v.lua", port),
				Listen:       fmt.Sprintf("127.0.0.1:%v", port),
				User:         "admin",
				Pass:         "pass",
				ConnectRetry: 5,
				RetryTimeout: 1 * time.Second,
				WaitStart:    time.Duration(i * int(10*time.Second)),
			})
			if err != nil {
				errChan <- err
			}
		}(i, p)
	}
	wg.Wait()
	select {
	case err := <-errChan:
		return nil, nil, err
	default:
	}
	return func() {
		for _, tt := range tts {
			test_helpers.StopTarantoolWithCleanup(tt)
		}
	}, tts, nil
}
