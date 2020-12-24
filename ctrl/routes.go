package ctrl

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/julienschmidt/httprouter"
)

//ListenAndServe met en place les routes et lance le serveur
func ListenAndServe(ListenOn string) error {
	//point d'entrée du ws
	router := httprouter.New()

	root := "/cmdscheduler"

	//TODO protection https://github.com/gorilla/csrf
	//type Handle func(http.ResponseWriter, *http.Request, Params)
	router.GET(root+"/ping", ping) //healthcheck

	//CRUD users
	router.GET(root+"/users", secMiddleWare("*", apiUserList))          //liste (rep 200, 403)
	router.GET(root+"/users/:id", secMiddleWare("*", apiUserGet))       //get item (rep 200, 404 not found, 403)
	router.POST(root+"/users", secMiddleWare("*", apiUserCreate))       //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/users/:id", secMiddleWare("*", apiUserPut))       //update (200)
	router.DELETE(root+"/users/:id", secMiddleWare("*", apiUserDelete)) //update (200)

	//interro factures
	//router.GET("/invoice", invoiceLst)
	//router.GET("/invoice/v1", invoiceLst)

	//ping pour tests micro service online
	//router.GET("/invoice/ping", ping)

	//gestion des erreurs qui provoquerait un crash (panic)
	router.PanicHandler = panicHandler

	//mise en place de la gestion du ctrl-c
	server := &http.Server{ //server custom simplement pour avoir accés au shutdown
		Addr:    ListenOn,
		Handler: router,
	}
	//go routine en attente du ctrl-c
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		//ctrl-c emis
		<-interrupt
		log.Println("Ctrl-c, stopping...")
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		//arret serveur web
		err := server.Shutdown(ctx)
		//arret ticket maj cache
		if err != nil {
			log.Println("server.Shutdown:", err)
		}
	}()

	//lancement du serveur web
	return server.ListenAndServe()
}
