package apiserver

import (
	"fmt"
	"log"
	"net/http"
	"pkg/storage"
	"strconv"
	"sync"
)

type Api struct {
	csh  *storage.Cache
	name string
	wait *sync.WaitGroup
}

func NewApi(csh *storage.Cache) *Api {
	api := Api{}
	api.Init(csh)
	return &api
}

func (a *Api) Init(csh *storage.Cache) {
	a.csh = csh
	a.name = "API"

	a.wait = &sync.WaitGroup{}
	a.wait.Add(1)
	go a.StartServer()
}

func (a *Api) StartServer() {

	http.HandleFunc("/client", a.Handler)

	log.Printf("%s: Start HTTP server on port 8081:\n", a.name)

	http.ListenAndServe(":8081", nil)
}

func (a *Api) Handler(w http.ResponseWriter, r *http.Request) {
	clientId1 := r.URL.Query().Get("id")
	clientId, err := strconv.ParseInt(clientId1, 10, 64)
	if err != nil {
		log.Printf("%s: ошибка конвертации %s в число: %v\n", a.name, clientId1, err)
		return
	}

	log.Printf("%s: запрос Client из кеша/бд, ClientId: %v\n", a.name, clientId)

	clientOut, err := a.csh.GetClientByIdFromCache(clientId)
	if err != nil {
		log.Printf("%s: ошибка получения Client из базы данных: %v\n", a.name, err)
		return
	}

	fmt.Fprintf(w, "Id: %d, Name: %s, Number: %d", clientOut.Id, clientOut.Name, clientOut.Number)
}

func (a *Api) Finish() {
	log.Printf("%v: Выключение сервера...\n", a.name)

	a.wait.Done()
	a.wait.Wait()

	log.Printf("%v: Сервер успешно выключен!\n", a.name)
}
