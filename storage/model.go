package storage

type Client struct {
	Id     int64   `json:"id"`
	Name   string `json:"name"`
	Number int    `json:"number"`
}
