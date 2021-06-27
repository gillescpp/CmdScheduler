package ctrl

import (
	"CmdScheduler/dal"
	"CmdScheduler/schd"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

//apiTaskFlowGet handler get /taskflows/:id
func apiTaskFlowGet(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//inputs :
	id, _ := strconv.Atoi(p.ByName("id"))
	if id <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	//get dal
	resp, err := dal.TaskFlowGet(id)
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

//apiTaskFlowList handler get /taskflows
func apiTaskFlowList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// filtre extrait du get
	searchQ := dal.NewSearchQueryFromRequest(r, &dal.DbTaskFlow{}, false)

	//get liste
	_, resp, err := dal.TaskFlowList(searchQ)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	//retour ok
	writeStdJSONResp(w, http.StatusOK, resp)
}

//apiTaskFlowCreate handler post /taskflows
//si ok : create 201 (Created and contain an entity, and a Location header.) ou 200
func apiTaskFlowCreate(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//deserial input
	var elm dal.DbTaskFlow
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

	err = dal.TaskFlowInsert(&elm, 0)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//notif sched
	schd.UpdateSchedFromDb("DbTaskFlow", elm.ID)

	//retour ok : 201 created
	writeStdJSONCreated(w, r.URL.Path, strconv.Itoa(elm.ID))
}

//apiTaskFlowPut handler put /taskflows/:id
func apiTaskFlowPut(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//deserial input
	var elm dal.DbTaskFlow
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

	err = dal.TaskFlowUpdate(elm, 0, true)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//notif sched
	schd.UpdateSchedFromDb("DbTaskFlow", elm.ID)

	//retour ok : 200
	writeStdJSONOK(w)
}

//apiTaskFlowDelete handler delete /taskflows/:id
func apiTaskFlowDelete(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	elmID, _ := strconv.Atoi(p.ByName("id"))
	if elmID <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	elm, err := dal.TaskFlowGet(elmID)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}
	if elm.ID > 0 {
		err = dal.TaskFlowDelete(elm.ID, 0)
		if err != nil {
			writeStdJSONErrInternalServer(w, err.Error())
			return
		}
		//notif sched
		schd.UpdateSchedFromDb("DbTaskFlow", elm.ID)
	}
	//retour ok : 200
	writeStdJSONOK(w)
}
