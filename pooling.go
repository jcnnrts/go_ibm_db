package go_ibm_db

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

//DBP struct type contains the timeout, dbinstance and connection string
type DBP struct {
	sql.DB
	con  string
	n    time.Duration
	pool *Pool
}

//Pool struct contais the about the pool like size, used and available connections
type Pool struct {
	availablePool map[string][]*DBP
	usedPool      map[string][]*DBP
	maxPoolSize   int
	curPoolSize   int
	mu            sync.Mutex
}

//Pconnect will return the pool instance
func Pconnect(poolSize int) *Pool {
	p := &Pool{
		availablePool: make(map[string][]*DBP),
		usedPool:      make(map[string][]*DBP),
		maxPoolSize:   poolSize,
		curPoolSize:   0,
	}

	return p
}

//Open will check for the connection in the pool
//If not opens a new connection and stores in the pool
func (p *Pool) Open(Connstr string, options ...string) *DBP {
	p.mu.Lock()
	defer p.mu.Unlock()

	var Time time.Duration
	var ConnMaxLifeTime int

	//Check if there are options
	if len(options) > 0 {

		for i := 0; i < len(options); i++ {
			opt := strings.Split(options[i], "=")
			if opt[0] == "SetConnMaxLifetime" {
				ConnMaxLifeTime, _ = strconv.Atoi(opt[1])
				Time = time.Duration(ConnMaxLifeTime) * time.Second
			} else {
				fmt.Println("not a valid parameter")
			}
		}
	} else {
		Time = 30 * time.Second
	}

	if p.curPoolSize < p.maxPoolSize {
		p.curPoolSize++

		if val, ok := p.availablePool[Connstr]; ok {
			if len(val) > 1 {
				dbpo := val[0]
				copy(val[0:], val[1:])
				val[len(val)-1] = nil
				val = val[:len(val)-1]
				p.availablePool[Connstr] = val
				p.usedPool[Connstr] = append(p.usedPool[Connstr], dbpo)
				dbpo.SetConnMaxLifetime(Time)
				return dbpo
			} else {
				dbpo := val[0]
				p.usedPool[Connstr] = append(p.usedPool[Connstr], dbpo)
				delete(p.availablePool, Connstr)
				dbpo.SetConnMaxLifetime(Time)
				return dbpo
			}
		} else {
			db, err := sql.Open("go_ibm_db", Connstr)
			if err != nil {
				return nil
			}

			dbi := &DBP{
				DB:   *db,
				con:  Connstr,
				n:    Time,
				pool: p,
			}

			p.usedPool[Connstr] = append(p.usedPool[Connstr], dbi)
			dbi.SetConnMaxLifetime(Time)

			return dbi
		}

	} else {
		db, err := sql.Open("go_ibm_db", Connstr)
		if err != nil {
			return nil
		}

		dbi := &DBP{
			DB:   *db,
			con:  Connstr,
			pool: p,
		}

		return dbi
	}
}

//Close will make the connection available for the next release
func (d *DBP) Close() {
	d.pool.mu.Lock()
	defer d.pool.mu.Unlock()

	d.pool.curPoolSize--

	var pos int
	i := -1

	if valc, okc := d.pool.usedPool[d.con]; okc {
		if len(valc) > 1 {
			for _, b := range valc {
				i = i + 1
				if b == d {
					pos = i
				}
			}
			dbpc := valc[pos]
			copy(valc[pos:], valc[pos+1:])
			valc[len(valc)-1] = nil
			valc = valc[:len(valc)-1]
			d.pool.usedPool[d.con] = valc
			d.pool.availablePool[d.con] = append(d.pool.availablePool[d.con], dbpc)
		} else {
			dbpc := valc[0]
			d.pool.availablePool[d.con] = append(d.pool.availablePool[d.con], dbpc)
			delete(d.pool.usedPool, d.con)
		}
		go d.Timeout()
	} else {
		d.DB.Close()
	}
}

//Timeout for closing the connection in pool
func (d *DBP) Timeout() {
	d.pool.mu.Lock()
	defer d.pool.mu.Unlock()

	var pos int
	i := -1
	select {
	case <-time.After(d.n):
		if valt, okt := d.pool.availablePool[d.con]; okt {
			if len(valt) > 1 {
				for _, b := range valt {
					i = i + 1
					if b == d {
						pos = i
					}
				}
				dbpt := valt[pos]
				copy(valt[pos:], valt[pos+1:])
				valt[len(valt)-1] = nil
				valt = valt[:len(valt)-1]
				d.pool.availablePool[d.con] = valt
				dbpt.DB.Close()
			} else {
				dbpt := valt[0]
				dbpt.DB.Close()
				delete(d.pool.availablePool, d.con)
			}
		}
	}
}

//Release will close all the connections in the pool
func (p *Pool) Release() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.availablePool != nil {
		for _, vala := range p.availablePool {
			for _, dbpr := range vala {
				dbpr.DB.Close()
			}
		}
		p.availablePool = nil
	}
	if p.usedPool != nil {
		for _, valu := range p.usedPool {
			for _, dbpr := range valu {
				dbpr.DB.Close()
			}
		}
		p.usedPool = nil
	}
}

// Display will print the  values in the map
func (p *Pool) Display() {
	fmt.Println(p.availablePool)
	fmt.Println(p.usedPool)
	fmt.Println(p.maxPoolSize)
	fmt.Println(p.curPoolSize)
}
