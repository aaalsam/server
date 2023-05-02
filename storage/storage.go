package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

type Storage struct {
	csh  *Cache
	db   *sql.DB
	name string
}

func NewStorage() *Storage {
	st := Storage{}
	st.db = st.InitDb()

	return &st
}

func (st *Storage) SetCahceInstance(csh *Cache) {
	st.csh = csh
}

func (st *Storage) InitDb() *sql.DB {
	st.name = "Postgres"

	dbUrl := fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable", "localhost", 5432, "postgres", "140399", "client")
	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		log.Fatalf("%v: ошибка подключения: %s\n", st.name, err)
	}
	db.Ping()
	if err != nil {
		log.Fatalf("%v: неудалось проверить подкл к БД: %s\n", st.name, err)
	}

	st.db = db

	log.Printf("%v: база данных подключена\n", st.name)

	return db
}

func (st *Storage) AddClient(cl Client) (int64, error) {
	var id int64

	err := st.db.QueryRow(`INSERT INTO clients (id, name, number) VALUES ($1, $2, $3) RETURNING id`, cl.Id, cl.Name, cl.Number).Scan(&id) //копирует возвращаемое значение в переменную с помощью указателя.

	if err != nil {
		log.Printf("%v: невозможно добавить данные (табл.clients): %v\n", st.name, err)
		return -1, err
	}

	log.Printf("%v: Клиент успешно добавлен в БД\n", st.name)

	st.csh.SetClient(id, cl)
	return id, nil
}

func (st *Storage) GetClientByIdFromDB(i int64) (Client, error) {
	var c Client

	err := st.db.QueryRow(`SELECT id, name, number FROM clients WHERE id = $1`, i).Scan(&c.Id, &c.Name, &c.Number)
	if err != nil {
		return c, errors.New("невозможно получить клиента из базы")
	}

	return c, nil
}

func (st *Storage) GetCacheState(bufSize int) (map[int64]Client, []int64, int, error) {
	buffer := make(map[int64]Client, bufSize)
	queue := make([]int64, bufSize)
	var queueInd int

	rows, err := st.db.Query("SELECT id FROM clients")
	if err != nil {
		log.Printf("%v: невозможно получить id из БД: %v\n", st.name, err)
	}
	defer rows.Close()

	var cid int64

	for rows.Next() {
		if err := rows.Scan(&cid); err != nil {
			log.Printf("%v: не удалось получить данные из БД: %v\n", st.name, err)
			return buffer, queue, queueInd, errors.New("не удалось получить данные из БД")
		}

		queue[queueInd] = cid
		queueInd++

		o, err := st.GetClientByIdFromDB(cid)
		if err != nil {
			log.Printf("%v: невозможно получить client из БД: %v\n", st.name, err)
			continue
		}
		buffer[cid] = o
	}

	if queueInd == 0 {
		return buffer, queue, queueInd, errors.New("кэш пустой")
	}

	return buffer, queue, queueInd, nil
}
