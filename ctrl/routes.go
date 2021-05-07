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
	router.DELETE(root+"/users/:id", secMiddleWare("*", apiUserDelete)) //delete (200)

	//CRUD agents
	router.GET(root+"/agents", secMiddleWare("*", apiAgentList))           //liste (rep 200, 403)
	router.GET(root+"/agents/:id", secMiddleWare("*", apiAgentGet))        //get item (rep 200, 404 not found, 403)
	router.POST(root+"/agents", secMiddleWare("*", apiAgentCreate))        //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/agents/:id", secMiddleWare("*", apiAgentPut))        //update (200)
	router.DELETE(root+"/agents/:id", secMiddleWare("*", apiAgentDelete))  //delete (200)
	router.POST(root+"/agents/eval", secMiddleWare("*", apiAgentEvaluate)) //eval d'un agent

	//CRUD queues
	router.GET(root+"/queues", secMiddleWare("*", apiQueueList))          //liste (rep 200, 403)
	router.GET(root+"/queues/:id", secMiddleWare("*", apiQueueGet))       //get item (rep 200, 404 not found, 403)
	router.POST(root+"/queues", secMiddleWare("*", apiQueueCreate))       //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/queues/:id", secMiddleWare("*", apiQueuePut))       //update (200)
	router.DELETE(root+"/queues/:id", secMiddleWare("*", apiQueueDelete)) //delete (200)

	//CRUD tags
	router.GET(root+"/tags", secMiddleWare("*", apiTagList))          //liste (rep 200, 403)
	router.GET(root+"/tags/:id", secMiddleWare("*", apiTagGet))       //get item (rep 200, 404 not found, 403)
	router.POST(root+"/tags", secMiddleWare("*", apiTagCreate))       //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/tags/:id", secMiddleWare("*", apiTagPut))       //update (200)
	router.DELETE(root+"/tags/:id", secMiddleWare("*", apiTagDelete)) //delete (200)

	//CRUD tasks
	router.GET(root+"/tasks", secMiddleWare("*", apiTaskList))          //liste (rep 200, 403)
	router.GET(root+"/tasks/:id", secMiddleWare("*", apiTaskGet))       //get item (rep 200, 404 not found, 403)
	router.POST(root+"/tasks", secMiddleWare("*", apiTaskCreate))       //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/tasks/:id", secMiddleWare("*", apiTaskPut))       //update (200)
	router.DELETE(root+"/tasks/:id", secMiddleWare("*", apiTaskDelete)) //delete (200)

	//SET/GET/LIST configs
	router.GET(root+"/cfgs", secMiddleWare("*", apiCfgList))    //liste (rep 200, 403)
	router.GET(root+"/cfgs/:id", secMiddleWare("*", apiCfgGet)) //get item (rep 200, 404 not found, 403)
	router.POST(root+"/cfgs", secMiddleWare("*", apiCfgPost))   //200

	//CRUD scheds
	router.GET(root+"/scheds", secMiddleWare("*", apiSchedList))          //liste (rep 200, 403)
	router.GET(root+"/scheds/:id", secMiddleWare("*", apiSchedGet))       //get item (rep 200, 404 not found, 403)
	router.POST(root+"/scheds", secMiddleWare("*", apiSchedCreate))       //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/scheds/:id", secMiddleWare("*", apiSchedPut))       //update (200)
	router.DELETE(root+"/scheds/:id", secMiddleWare("*", apiSchedDelete)) //delete (200)

	//CRUD taskflows
	router.GET(root+"/taskflows", secMiddleWare("*", apiTaskFlowList))          //liste (rep 200, 403)
	router.GET(root+"/taskflows/:id", secMiddleWare("*", apiTaskFlowGet))       //get item (rep 200, 404 not found, 403)
	router.POST(root+"/taskflows", secMiddleWare("*", apiTaskFlowCreate))       //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/taskflows/:id", secMiddleWare("*", apiTaskFlowPut))       //update (200)
	router.DELETE(root+"/taskflows/:id", secMiddleWare("*", apiTaskFlowDelete)) //delete (200)

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
