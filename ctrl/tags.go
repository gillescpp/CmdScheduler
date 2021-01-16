package ctrl

import (
	"CmdScheduler/dal"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

//apiTagGet handler get /tags/:id
func apiTagGet(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//inputs :
	id, _ := strconv.Atoi(p.ByName("id"))
	if id <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	//get dal
	resp, err := dal.TagGet(id)
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

//apiTagList handler get /tags
func apiTagList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// filtre extrait du get
	searchQ := dal.NewSearchQueryFromRequest(r, &dal.DbTag{}, false)

	//get liste
	_, resp, err := dal.TagList(searchQ)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	//retour ok
	writeStdJSONResp(w, http.StatusOK, resp)
}

//apiTagCreate handler post /tags
//si ok : create 201 (Created and contain an entity, and a Location header.) ou 200
func apiTagCreate(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//deserial input
	var elm dal.DbTag
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

	err = dal.TagInsert(&elm, 0)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	//retour ok : 201 created
	writeStdJSONCreated(w, r.URL.Path+"/"+strconv.Itoa(elm.ID))
}

//apiTagPut handler put /tags/:id
func apiTagPut(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//deserial input
	var elm dal.DbTag
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

	err = dal.TagUpdate(elm, 0)
	if err != nil {
		writeStdJSONErrInternalServer(w, err.Error())
		return
	}
	//retour ok : 200
	writeStdJSONOK(w)
}

//apiTagDelete handler delete /tags/:id
func apiTagDelete(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	elmID, _ := strconv.Atoi(p.ByName("id"))
	if elmID <= 0 {
		writeStdJSONErrBadRequest(w, "invalid id")
		return
	}

	elm, err := dal.TagGet(elmID)
	if err != nil {
		writeStdJSONErrBadRequest(w, err.Error())
		return
	}
	if elm.ID > 0 {
		err = dal.TagDelete(elm.ID, 0)
		if err != nil {
			writeStdJSONErrInternalServer(w, err.Error())
			return
		}
	}
	//retour ok : 200
	writeStdJSONOK(w)
}
