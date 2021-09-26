package ctrl

import (
	"CmdScheduler/dal"
	"CmdScheduler/schd"
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

type IdOnly struct {
	ID int `json:"id"`
}

//apiManualLaunchTF handler post /queues
func apiManualLaunchTF(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//deserial input
	var elm IdOnly
	err := json.NewDecoder(r.Body).Decode(&elm)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}
	if elm.ID <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	//check existance tf
	tf, _ := dal.TaskFlowGet(elm.ID)
	if tf.ID == 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	// recup session user
	s := getSessionFromCtx(r)

	// tf non coché "lancement manu autorisé", interdit sauf droit task builder ou +
	if !tf.ManualLaunch && s.RightLevel < dal.RightLvlTaskBuilder {
		writeStdJSONErrBadRequest(w, "this taskflow cannot be launched manually")
		return
	}

	//insection directe taskflow
	schd.ManualLaunchTF(elm.ID, s.Login)

	//retour ok : 201 created
	writeStdJSONOK(w, nil)
}

//apiGetQueuesStates info encours scheduleur
func apiGetQueuesStates(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	qstate := schd.GetViewState()

	writeStdJSONOK(w, &qstate)
}
