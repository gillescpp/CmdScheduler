package ctrl

import (
	"CmdScheduler/dal"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
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
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(usr)
}

//apiUserList handler get /users
func apiUserList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	resq := dal.NewSearchQueryFromRequest(r, &dal.DbUser{}, false)

	fmt.Println(r.URL.RawQuery)
	fmt.Println(resq.Offset, "limit:", resq.Limit, "sort", resq.SQLSort)
	fmt.Println(resq.SQLFilter)
	for _, c := range resq.SQLParams {
		s := reflect.ValueOf(c).Elem().String()
		fmt.Printf("%t %v %v", c, c, s)
	}

	//inputs :
	/*
		paging := extractPaging(r)
		filter := extractFilter(r, []string{"login", "name"})

		//get liste date
		usrs, err := dal.UserList(paging, filter)
		if err != nil {
			writeStdJSONErrInternalServer(w, err.Error())
			return
		}

		//retour ok
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(usrs)
	*/
	json.NewEncoder(w).Encode(nil)
}

//apiUserCreate handler post /users
func apiUserCreate(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Fprintf(w, "pong")
}

//apiUserPut handler put /users/:id
func apiUserPut(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Fprintf(w, "pong")
}

//apiUserDelete handler delete /users/:id
func apiUserDelete(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Fprintf(w, "pong")
}
