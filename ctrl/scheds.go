package ctrl

import (
	"CmdScheduler/dal"
	"CmdScheduler/schd"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

//apiSchedGet handler get /scheds/:id
func apiSchedGet(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//inputs :
	id, _ := strconv.Atoi(p.ByName("id"))
	if id <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	//get dal
	resp, err := dal.SchedGet(id)
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

//apiSchedList handler get /scheds
func apiSchedList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// filtre extrait du get
	searchQ := dal.NewSearchQueryFromRequest(r, &dal.DbSched{}, false)

	//get liste
	_, resp, err := dal.SchedList(searchQ)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	//retour ok
	writeStdJSONResp(w, http.StatusOK, resp)
}

//apiSchedCreate handler post /scheds
//si ok : create 201 (Created and contain an entity, and a Location header.) ou 200
func apiSchedCreate(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//deserial input
	var elm dal.DbSched
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

	err = dal.SchedInsert(&elm, 0)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//noti sched
	schd.UpdateSchedFromDb("DbSched", elm.ID)

	//retour ok : 201 created
	writeStdJSONCreated(w, r.URL.Path+"/"+strconv.Itoa(elm.ID))
}

//apiSchedPut handler put /scheds/:id
func apiSchedPut(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//deserial input
	var elm dal.DbSched
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

	err = dal.SchedUpdate(elm, 0, true)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//noti sched
	schd.UpdateSchedFromDb("DbSched", elm.ID)

	//retour ok : 200
	writeStdJSONOK(w)
}

//apiSchedDelete handler delete /scheds/:id
func apiSchedDelete(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	elmID, _ := strconv.Atoi(p.ByName("id"))
	if elmID <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	elm, err := dal.SchedGet(elmID)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}
	if elm.ID > 0 {
		err = dal.SchedDelete(elm.ID, 0)
		if err != nil {
			writeStdJSONErrInternalServer(w, err.Error())
			return
		}
	}

	//noti sched
	schd.UpdateSchedFromDb("DbSched", elm.ID)

	//retour ok : 200
	writeStdJSONOK(w)
}
