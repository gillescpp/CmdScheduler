package ctrl

import (
	"CmdScheduler/dal"
	"CmdScheduler/schd"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

//apiQueueGet handler get /queues/:id
func apiQueueGet(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//inputs :
	id, _ := strconv.Atoi(p.ByName("id"))
	if id <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	//get dal
	resp, err := dal.QueueGet(id)
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

//apiQueueList handler get /queues
func apiQueueList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// filtre extrait du get
	searchQ := dal.NewSearchQueryFromRequest(r, &dal.DbQueue{}, false)

	//get liste
	_, resp, err := dal.QueueList(searchQ)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	//retour ok
	writeStdJSONResp(w, http.StatusOK, resp)
}

//apiQueueCreate handler post /queues
//si ok : create 201 (Created and contain an entity, and a Location header.) ou 200
func apiQueueCreate(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//deserial input
	var elm dal.DbQueue
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

	err = dal.QueueInsert(&elm, 0)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//notif sched
	schd.UpdateSchedFromDb("DbQueue", elm.ID)

	//retour ok : 201 created
	writeStdJSONCreated(w, r.URL.Path+"/"+strconv.Itoa(elm.ID))
}

//apiQueuePut handler put /queues/:id
func apiQueuePut(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//deserial input
	var elm dal.DbQueue
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

	err = dal.QueueUpdate(elm, 0, false)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//notif sched
	schd.UpdateSchedFromDb("DbQueue", elm.ID)

	//retour ok : 200
	writeStdJSONOK(w)
}

//apiQueueDelete handler delete /queues/:id
func apiQueueDelete(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	elmID, _ := strconv.Atoi(p.ByName("id"))
	if elmID <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	elm, err := dal.QueueGet(elmID)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}
	if elm.ID > 0 {
		err = dal.QueueDelete(elm.ID, 0)
		if err != nil {
			writeStdJSONErrInternalServer(w, err.Error())
			return
		}
		//notif sched
		schd.UpdateSchedFromDb("DbQueue", elm.ID)
	}
	//retour ok : 200
	writeStdJSONOK(w)
}
