package ctrl

import (
	"CmdScheduler/dal"
	"CmdScheduler/schd"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

//apiAgentGet handler get /agents/:id
func apiAgentGet(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//inputs :
	id, _ := strconv.Atoi(p.ByName("id"))
	if id <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	//get dal
	resp, err := dal.AgentGet(id)
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

//apiAgentList handler get /agents
func apiAgentList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// filtre extrait du get
	searchQ := dal.NewSearchQueryFromRequest(r, &dal.DbAgent{}, false)

	//get liste
	_, resp, err := dal.AgentList(searchQ)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	//retour ok
	writeStdJSONResp(w, http.StatusOK, resp)
}

//apiAgentCreate handler post /agents
//si ok : create 201 (Created and contain an entity, and a Location header.) ou 200
func apiAgentCreate(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//deserial input
	var elm dal.DbAgent
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

	err = dal.AgentInsert(&elm, getUsrIdFromCtx(r))
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//notif sched
	schd.UpdateSchedFromDb("DbAgent", elm.ID)

	elm, err = dal.AgentGet(elm.ID) //reprise valeur sur bdd pour champ calc ou autre val par defaut
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//retour ok : 201 created
	writeStdJSONCreated(w, r.URL.Path, strconv.Itoa(elm.ID), &elm)
}

//apiAgentPut handler put /agents/:id
func apiAgentPut(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//deserial input
	var elm dal.DbAgent
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

	err = dal.AgentUpdate(elm, getUsrIdFromCtx(r), nil)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//notif sched
	schd.UpdateSchedFromDb("DbAgent", elm.ID)

	elm, err = dal.AgentGet(elm.ID) //reprise valeur sur bdd pour champ calc ou autre val par defaut
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//retour ok : 200
	writeStdJSONOK(w, &elm)
}

//apiAgentDelete handler delete /agents/:id
func apiAgentDelete(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	elmID, _ := strconv.Atoi(p.ByName("id"))
	if elmID <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	elm, err := dal.AgentGet(elmID)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}
	if elm.ID > 0 {
		err = dal.AgentDelete(elm.ID, getUsrIdFromCtx(r))
		if err != nil {
			writeStdJSONErrInternalServer(w, err.Error())
			return
		}
		//notif sched
		schd.UpdateSchedFromDb("DbAgent", elm.ID)
	}
	//retour ok : 200
	writeStdJSONOK(w, nil)
}

//apiAgentEvaluate permet de tester une agent
func apiAgentEvaluate(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//deserial input
	var elm dal.DbAgent
	err := json.NewDecoder(r.Body).Decode(&elm)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}

	err = elm.Validate(elm.ID == 0)
	if err != nil {
		elm.EvalResultInfo = err.Error()
	} else {
		err = elm.Evaluate()
		if err != nil {
			elm.EvalResultInfo = err.Error()
		}
	}

	writeStdJSONResp(w, http.StatusOK, elm)
}
