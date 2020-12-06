package serverd

import (
	"log"
	"net/http"

	"github.com/paragor/parabase/pkg/engine"
)

type Serverd struct {
	engine engine.StorageEngine
}

func NewServerd(engine engine.StorageEngine) *Serverd {
	return &Serverd{engine: engine}
}

const apiV1Prefix = "/api/v1/"

func (s *Serverd) Run() error {
	server := &http.ServeMux{}
	server.HandleFunc(apiV1Prefix+"get", logRequestMiddleware(s.httpGet))
	server.HandleFunc(apiV1Prefix+"set", logRequestMiddleware(s.httpSet))
	return http.ListenAndServe(":8080", server)
}

func (s *Serverd) httpGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != "POST" {
		stringResponse(writer, 400, "ONLY POST ALLOW")
		return
	}
	err := request.ParseForm()
	if err != nil {
		stringResponse(writer, 400, err.Error())
		return
	}
	result, err := s.engine.Get([]byte(request.PostForm.Get("key")))
	if err != nil {
		stringResponse(writer, 500, err.Error())
		return
	}

	stringResponse(writer, 200, string(result))
}

func (s *Serverd) httpSet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != "POST" {
		stringResponse(writer, 400, "ONLY POST ALLOW")
		return
	}
	err := request.ParseForm()
	if err != nil {
		stringResponse(writer, 400, err.Error())
		return
	}
	err = s.engine.Set([]byte(request.PostForm.Get("key")), []byte(request.PostForm.Get("value")))
	if err != nil {
		stringResponse(writer, 500, err.Error())
		return
	}

	stringResponse(writer, 200, "OK")
}

func stringResponse(writer http.ResponseWriter, statusCode int, msg string) {
	writer.WriteHeader(statusCode)
	_, err := writer.Write([]byte(msg))
	if err != nil {
		log.Println(err)
	}
}

func logRequestMiddleware(handlerFunc http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		log.Printf("%s > \n", request.URL.String())
		handlerFunc(writer, request)
	}
}
