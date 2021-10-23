package ctrl

import (
	"CmdScheduler/dal"
	"CmdScheduler/schd"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

//apiTaskGet handler get /tasks/:id
func apiTaskGet(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//inputs :
	id, _ := strconv.Atoi(p.ByName("id"))
	if id <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	//get dal
	resp, err := dal.TaskGet(id)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	if resp.ID == 0 {
		writeStdJSONErrNotFound(w, "id not found")
		return
	}

	//retour ok
	writeStdJSONResp(w, http.StatusOK, resp)
}

//apiTaskList handler get /tasks
func apiTaskList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// filtre extrait du get
	searchQ := dal.NewSearchQueryFromRequest(r, &dal.DbTask{}, false)

	//get liste
	_, resp, err := dal.TaskList(searchQ)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	//retour ok
	writeStdJSONResp(w, http.StatusOK, resp)
}

//apiTaskCreate handler post /tasks
//si ok : create 201 (Created and contain an entity, and a Location header.) ou 200
func apiTaskCreate(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//deserial input
	var elm dal.DbTask
	err := json.NewDecoder(r.Body).Decode(&elm)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}

	err = elm.Validate(true)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}

	err = dal.TaskInsert(&elm, getUsrIdFromCtx(r))
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//notif sched
	schd.UpdateSchedFromDb("DbTask", elm.ID)

	elm, err = dal.TaskGet(elm.ID) //reprise valeur sur bdd pour champ calc ou autre val par defaut
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//retour ok : 201 created
	writeStdJSONCreated(w, r.URL.Path, strconv.Itoa(elm.ID), &elm)
}

//apiTaskPut handler put /tasks/:id
func apiTaskPut(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//deserial input
	var elm dal.DbTask
	err := json.NewDecoder(r.Body).Decode(&elm)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}
	elm.ID, _ = strconv.Atoi(p.ByName("id"))

	err = elm.Validate(false)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}

	err = dal.TaskUpdate(elm, getUsrIdFromCtx(r), nil)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//notif sched
	schd.UpdateSchedFromDb("DbTask", elm.ID)

	elm, err = dal.TaskGet(elm.ID) //reprise valeur sur bdd pour champ calc ou autre val par defaut
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//retour ok : 200
	writeStdJSONOK(w, &elm)
}

//apiTaskDelete handler delete /tasks/:id
func apiTaskDelete(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	elmID, _ := strconv.Atoi(p.ByName("id"))
	if elmID <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	elm, err := dal.TaskGet(elmID)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}
	if elm.ID > 0 {
		err = dal.TaskDelete(elm.ID, getUsrIdFromCtx(r))
		if err != nil {
			writeStdJSONErrInternalServer(w, err.Error())
			return
		}
		//notif sched
		schd.UpdateSchedFromDb("DbTask", elm.ID)
	}
	//retour ok : 200
	writeStdJSONOK(w, nil)
}
