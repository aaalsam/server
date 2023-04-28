package storage

import (
	"log"
	"sync"
)

type Cache struct {
	buffer  map[int64]Client
	queue   []int64
	bufSize int
	pos     int
	DBInst  *Storage
	name    string
	mutex   *sync.RWMutex
}

func NewCache(st *Storage) *Cache {
	csh := Cache{}
	csh.Init(st)
	return &csh
}

func (c *Cache) Init(st *Storage) {
	c.DBInst = st
	st.SetCahceInstance(c)
	c.name = "Cache"
	c.mutex = &sync.RWMutex{}
	c.bufSize = 10
	c.buffer = make(map[int64]Client, c.bufSize)
	c.queue = make([]int64, c.bufSize)

	c.getCacheFromDatabase()
}

func (c *Cache) getCacheFromDatabase() {
	log.Printf("%v: загрузка кэша из БД\n", c.name)
	buf, queue, pos, err := c.DBInst.GetCacheState(c.bufSize)
	if err != nil {
		log.Printf("%s: загрузка кэша из БД: невозможно загрузить из БД или в БД ничего нет: %v\n", c.name, err)
		return
	}

	if pos == c.bufSize {
		pos = 0
	}

	c.mutex.Lock()
	c.buffer = buf
	c.queue = queue
	c.pos = pos
	c.mutex.Unlock()
	log.Printf("%s: кэш загружен из БД", c.name)
}

func (c *Cache) SetClient(i int64, cl Client) {
	if c.bufSize > 0 {
		c.mutex.Lock()
		c.queue[c.pos] = i
		c.pos++
		if c.pos == c.bufSize {
			c.pos = 0
		}

		c.buffer[i] = cl
		c.mutex.Unlock()

		log.Printf("%s: клиент успешно добавлен в кэш\n", c.name)
	} else {
		log.Printf("%s: кэш переполнен\n", c.name)
	}
}

func (c *Cache) GetClientByIdFromCache(i int64) (*Client, error) {
	var cl *Client = &Client{}
	var cl1 Client
	var err error

	c.mutex.RLock()

	cl1, isExist := c.buffer[i]
	c.mutex.RUnlock()

	if isExist {
		log.Printf("%s: ClientId (id:%d) взят из кеша!\n", c.name, i)
	} else {
		cl1, err = c.DBInst.GetClientByIdFromDB(i)
		if err != nil {
			log.Printf("%s: GetClientById(): ошибка получения Client: %v\n", c.name, err)
			return cl, err
		}
		c.SetClient(i, cl1)
		log.Printf("%s: ClientId (id:%d) взят из бд и сохранен в кеш!\n", c.name, i)
	}

	cl.Id = cl1.Id
	cl.Name = cl1.Name
	cl.Number = cl1.Number
	return cl, nil
}
