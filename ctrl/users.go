package ctrl

import (
	"CmdScheduler/dal"
	"CmdScheduler/sessions"
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
	elm, err := dal.UserGet(id)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	if elm.ID == 0 {
		writeStdJSONErrNotFound(w, "id not found")
		return
	}

	//retour ok
	writeStdJSONResp(w, http.StatusOK, elm)
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
	var elm dal.DbUser
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

	err = dal.UserInsert(&elm, getUsrIdFromCtx(r))
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	elm, err = dal.UserGet(elm.ID) //reprise valeur sur bdd pour champ calc ou autre val par defaut
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//retour ok : 201 created
	writeStdJSONCreated(w, r.URL.Path, strconv.Itoa(elm.ID), &elm)
}

//apiUserPut handler put /users/:id
func apiUserPut(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//deserial input
	var elm dal.DbUser
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

	err = dal.UserUpdate(elm, getUsrIdFromCtx(r))
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//ras sessions concernés par cet usr id, sauf cas de l'user qui modifie lui même
	if elm.ID != getUsrIdFromCtx(r) {
		removeSessionByUsrId(elm.ID)
	}

	elm, err = dal.UserGet(elm.ID) //reprise valeur sur bdd pour champ calc ou autre val par defaut
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}

	//retour ok : 200
	writeStdJSONOK(w, &elm)
}

//apiUserDelete handler delete /users/:id
func apiUserDelete(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	usID, _ := strconv.Atoi(p.ByName("id"))
	if usID <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	elm, err := dal.UserGet(usID)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}
	if elm.ID > 0 {
		err = dal.UserDelete(elm.ID, getUsrIdFromCtx(r))
		if err != nil {
			writeStdJSONErrInternalServer(w, err.Error())
			return
		}
		//ras sessions concernés par cet usr id, sauf cas de l'user qui modifie lui même
		if elm.ID != getUsrIdFromCtx(r) {
			removeSessionByUsrId(elm.ID)
		}
	}
	//retour ok : 200
	writeStdJSONOK(w, nil)
}

//removeSessionByUsrId supprimer la/les session d'un utilisateur donnée
// (utilisé en cas de modif pour ne pas laisser des clients connecté sur d'anciens drois )
func removeSessionByUsrId(usrID int) {
	sessionLst := sessions.List()
	for token, s := range sessionLst {
		if s.Data["USRID"].(int) == usrID {
			sessions.Remove(token)
			break
		}
	}
}
