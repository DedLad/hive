package models

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
