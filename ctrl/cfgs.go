package ctrl

import (
	"CmdScheduler/dal"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
)

//apiCfgGet handler get /cfgs/:id
func apiCfgGet(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//inputs :
	key := strings.ToLower(strings.TrimSpace(p.ByName("id")))
	if key == "" {
		writeStdJSONErrBadRequest(w, "invalid key")
		return
	}

	//get dal
	resp, err := dal.CfgKVGet(key)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//retour ok
	writeStdJSONResp(w, http.StatusOK, &dal.KVJSON{
		Key:   key,
		Value: resp,
	})
}

//apiCfgList handler get /cfgs
func apiCfgList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//get liste
	resp, err := dal.CfgKVList()
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	//retour ok
	writeStdJSONResp(w, http.StatusOK, resp)
}

//apiCfgPost handler post /cfgs
func apiCfgPost(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//deserial input
	var elm dal.KVJSON
	err := json.NewDecoder(r.Body).Decode(&elm)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}

	if elm.Key == "" {
		writeStdJSONErrBadRequest(w, "empty key forbidden")
		return
	}
	if dal.CfgKVIsSystem(elm.Key) {
		writeStdJSONErrForbidden(w, "system key is read only")
		return
	}

	err = dal.CfgKVSet(elm.Key, elm.Value)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	//retour ok : 200
	writeStdJSONOK(w, &elm)
}
