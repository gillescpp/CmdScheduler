package ctrl

import (
	"CmdScheduler/dal"
	"CmdScheduler/sessions"
	"CmdScheduler/slog"
	"context"
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

	// Auth pour obtention token api
	router.POST(root+"/auth", secMiddleWare("", nil, true, apiAuth))            //200, 401
	router.GET(root+"/disconnect", secMiddleWare("", nil, true, apiDisconnect)) //200

	// user en cours
	router.GET(root+"/my/right", secMiddleWare("", nil, true, apiGetRightList)) //200, 401

	//info dash
	router.GET(root+"/queue/state", secMiddleWare("QUEUE", nil, true, apiGetQueuesStates)) //200

	//CRUD users
	router.GET(root+"/users", secMiddleWare("USER", nil, true, apiUserList))          //liste (rep 200, 403)
	router.GET(root+"/users/:id", secMiddleWare("USER", nil, true, apiUserGet))       //get item (rep 200, 404 not found, 403)
	router.POST(root+"/users", secMiddleWare("USER", nil, true, apiUserCreate))       //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/users/:id", secMiddleWare("USER", nil, true, apiUserPut))       //update (200)
	router.DELETE(root+"/users/:id", secMiddleWare("USER", nil, true, apiUserDelete)) //delete (200)

	//CRUD agents
	router.GET(root+"/agents", secMiddleWare("AGENT", nil, true, apiAgentList))           //liste (rep 200, 403)
	router.GET(root+"/agents/:id", secMiddleWare("AGENT", nil, true, apiAgentGet))        //get item (rep 200, 404 not found, 403)
	router.POST(root+"/agents", secMiddleWare("AGENT", nil, true, apiAgentCreate))        //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/agents/:id", secMiddleWare("AGENT", nil, true, apiAgentPut))        //update (200)
	router.DELETE(root+"/agents/:id", secMiddleWare("AGENT", nil, true, apiAgentDelete))  //delete (200)
	router.POST(root+"/agents/eval", secMiddleWare("AGENT", nil, true, apiAgentEvaluate)) //eval d'un agent

	//CRUD queues
	router.GET(root+"/queues", secMiddleWare("QUEUE", nil, true, apiQueueList))          //liste (rep 200, 403)
	router.GET(root+"/queues/:id", secMiddleWare("QUEUE", nil, true, apiQueueGet))       //get item (rep 200, 404 not found, 403)
	router.POST(root+"/queues", secMiddleWare("QUEUE", nil, true, apiQueueCreate))       //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/queues/:id", secMiddleWare("QUEUE", nil, true, apiQueuePut))       //update (200)
	router.DELETE(root+"/queues/:id", secMiddleWare("QUEUE", nil, true, apiQueueDelete)) //delete (200)

	//CRUD tags
	router.GET(root+"/tags", secMiddleWare("TAGS", nil, true, apiTagList))          //liste (rep 200, 403)
	router.GET(root+"/tags/:id", secMiddleWare("TAGS", nil, true, apiTagGet))       //get item (rep 200, 404 not found, 403)
	router.POST(root+"/tags", secMiddleWare("TAGS", nil, true, apiTagCreate))       //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/tags/:id", secMiddleWare("TAGS", nil, true, apiTagPut))       //update (200)
	router.DELETE(root+"/tags/:id", secMiddleWare("TAGS", nil, true, apiTagDelete)) //delete (200)

	//CRUD tasks
	router.GET(root+"/tasks", secMiddleWare("TASK", nil, true, apiTaskList))          //liste (rep 200, 403)
	router.GET(root+"/tasks/:id", secMiddleWare("TASK", nil, true, apiTaskGet))       //get item (rep 200, 404 not found, 403)
	router.POST(root+"/tasks", secMiddleWare("TASK", nil, true, apiTaskCreate))       //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/tasks/:id", secMiddleWare("TASK", nil, true, apiTaskPut))       //update (200)
	router.DELETE(root+"/tasks/:id", secMiddleWare("TASK", nil, true, apiTaskDelete)) //delete (200)

	//SET/GET/LIST configs
	router.GET(root+"/cfgs", secMiddleWare("CONFIG", nil, true, apiCfgList))    //liste (rep 200, 403)
	router.GET(root+"/cfgs/:id", secMiddleWare("CONFIG", nil, true, apiCfgGet)) //get item (rep 200, 404 not found, 403)
	router.POST(root+"/cfgs", secMiddleWare("CONFIG", nil, true, apiCfgPost))   //200

	//CRUD scheds
	router.GET(root+"/scheds", secMiddleWare("SCHED", nil, true, apiSchedList))          //liste (rep 200, 403)
	router.GET(root+"/scheds/:id", secMiddleWare("SCHED", nil, true, apiSchedGet))       //get item (rep 200, 404 not found, 403)
	router.POST(root+"/scheds", secMiddleWare("SCHED", nil, true, apiSchedCreate))       //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/scheds/:id", secMiddleWare("SCHED", nil, true, apiSchedPut))       //update (200)
	router.DELETE(root+"/scheds/:id", secMiddleWare("SCHED", nil, true, apiSchedDelete)) //delete (200)

	//CRUD taskflows
	router.GET(root+"/taskflows", secMiddleWare("TASKFLOW", nil, true, apiTaskFlowList))          //liste (rep 200, 403)
	router.GET(root+"/taskflows/:id", secMiddleWare("TASKFLOW", nil, true, apiTaskFlowGet))       //get item (rep 200, 404 not found, 403)
	router.POST(root+"/taskflows", secMiddleWare("TASKFLOW", nil, true, apiTaskFlowCreate))       //create 201 (Created and contain an entity, and a Location header.) ou 200
	router.PUT(root+"/taskflows/:id", secMiddleWare("TASKFLOW", nil, true, apiTaskFlowPut))       //update (200)
	router.DELETE(root+"/taskflows/:id", secMiddleWare("TASKFLOW", nil, true, apiTaskFlowDelete)) //delete (200)

	// lancement de taskflow manuel
	router.POST(root+"/taskflows/launch", secMiddleWare("TASKFLOW", func(s *sessions.Session) bool {
		if s != nil && s.RightLevel >= dal.RightLvlTaskRunner {
			return true
		}
		return false
	}, true, apiManualLaunchTF)) //create 201 (Created and contain an entity, and a Location header.) ou 200

	//requete browser preflight cors
	router.OPTIONS(root+"/*path", secMiddleWare("", nil, true, nil))

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
		slog.Warning("main", "Ctrl-c, stopping...")
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		//arret serveur web
		err := server.Shutdown(ctx)
		//arret ticket maj cache
		if err != nil {
			slog.Trace("main", "Server shutdown.")
		}
	}()

	//lancement du serveur web
	return server.ListenAndServe()
}
