package ctrl

import (
	"CmdScheduler/dal"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

//apiUserGet handler get /users/:id
func apiUserGet(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//inputs :
	id, _ := strconv.Atoi(p.ByName("id"))
	if id <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	//get dal
	usr, err := dal.UserGet(id)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	if usr.ID == 0 {
		writeStdJSONErrNotFound(w, "id not found")
		return
	}

	//retour ok
	writeStdJSONResp(w, http.StatusOK, usr)
}

//apiUserList handler get /users
func apiUserList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	searchQ := dal.NewSearchQueryFromRequest(r, &dal.DbUser{}, false)

	//get liste
	_, resp, err := dal.UserList(searchQ)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	//retour ok
	writeStdJSONResp(w, http.StatusOK, resp)
}

//apiUserCreate handler post /users
//si ok : create 201 (Created and contain an entity, and a Location header.) ou 200
func apiUserCreate(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//deserial input
	var usr dal.DbUser
	err := json.NewDecoder(r.Body).Decode(&usr)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}

	err = usr.Validate(true)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}

	err = dal.UserInsert(&usr, 0) //todo : userid selon session user
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	//retour ok : 201 created
	writeStdJSONCreated(w, r.URL.Path+"/"+strconv.Itoa(usr.ID))
}

//apiUserPut handler put /users/:id
func apiUserPut(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//deserial input
	var usr dal.DbUser
	err := json.NewDecoder(r.Body).Decode(&usr)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}
	usr.ID, _ = strconv.Atoi(p.ByName("id"))

	err = usr.Validate(false)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}

	err = dal.UserUpdate(usr, 0) //todo : userid selon session user +mode admin, use std ne peu maj que son propre usid
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	//retour ok : 200
	writeStdJSONOK(w)
}

//apiUserDelete handler delete /users/:id
func apiUserDelete(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	usID, _ := strconv.Atoi(p.ByName("id"))
	if usID <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	usr, err := dal.UserGet(usID)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}
	if usr.ID > 0 {
		err = dal.UserDelete(usr.ID, 0) //todo : userid selon session
		if err != nil {
			writeStdJSONErrInternalServer(w, err.Error())
			return
		}
	}
	//retour ok : 200
	writeStdJSONOK(w)
}
