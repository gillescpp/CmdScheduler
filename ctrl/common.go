package ctrl

import (
	"CmdScheduler/dal"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
)

//variable globale controleur
var (
	SessionKey []byte //clé de cryptage cookie
)

//JSONStdResponse réponse json générique
type JSONStdResponse struct {
	Error  string `json:"errorMessage,omitempty"`
	ID     string `json:"Id,omitempty"`
	Result string `json:"Result,omitempty"`
}

// Securité : simples code action autorisé
// * : ADMIN : tous les droits, dont la gestion des uers
// T : TASKCREATOR : creer/modif des taches
// S : SCHEDULE : creer/modif des planif
// V : VIEWER : visu planning

// secMiddleWare middleware pour la gestion de la sécurité
// secMiddleWare middle
func secMiddleWare(requiredRoles string, handler httprouter.Handle) httprouter.Handle {
	////TODO : si pas loggué : redirection, si ko : 503
	return handler
}

// Helpers paging

//variable globale controleur
var (
	DefaultRecordPerPage = 25
	MaxRecordPerPage     = 100
)

//extractPaging recup element de paging dans le GET
func extractPaging(r *http.Request) dal.Paging {
	p := dal.Paging{
		Page:       1,
		Sort:       "",
		RowPerPage: DefaultRecordPerPage,
	}

	//extraction element de la requete
	npage, _ := strconv.Atoi(r.Form.Get("page"))  //page num
	limit, _ := strconv.Atoi(r.Form.Get("limit")) //page num
	sort := strings.TrimSpace(r.Form.Get("sort"))

	if limit > MaxRecordPerPage {
		limit = MaxRecordPerPage
	}

	if npage > 0 {
		p.Page = npage
	}
	if limit > 0 {
		p.RowPerPage = limit
	}
	p.Sort = sort

	return p
}

//extractFilter recup filtre dans le GET
func extractFilter(r *http.Request, qSearchOn []string) dal.Filter {
	f := dal.Filter{
		ShowDeleted:    false,
		OtherSQLFilter: "",
		OnlyThisID:     0,
	}

	q := strings.TrimSpace(r.Form.Get("q"))     //champ de recherche standard
	idspec, _ := strconv.Atoi(r.Form.Get("id")) //id en particulier
	sqlfilter := ""

	if idspec > 0 {
		sqlfilter += "id = ?" //TODO : integrer tableau de param pou les like
	} else if len(qSearchOn) > 0 && q != "" {
		sqlfilter += "("
		for _, f := range qSearchOn {
			sqlfilter += f + " like 'qqqqqq'" //TODO : integrer plutot un tableau de param pou les like
		}
		sqlfilter += ")"
	}
	f.OtherSQLFilter = sqlfilter

	return f
}

// Helpers pour les réponses API

//writeStdJSONResp output json réponse std
func writeStdJSONResp(w http.ResponseWriter, code int, payload JSONStdResponse) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(payload)
}

//writeStdJSONBadErrRequest erreur bad request
func writeStdJSONErrBadRequest(w http.ResponseWriter, errMsg string) {
	writeStdJSONResp(w, http.StatusBadRequest, JSONStdResponse{
		Error:  errMsg,
		Result: "ERROR",
	})
}

//writeStdJSONErrInternalServer erreur serveur
func writeStdJSONErrInternalServer(w http.ResponseWriter, errMsg string) {
	writeStdJSONResp(w, http.StatusInternalServerError, JSONStdResponse{
		Error:  errMsg,
		Result: "ERROR",
	})
}

//writeStdJSONErrNotFound erreur applicative
func writeStdJSONErrNotFound(w http.ResponseWriter, errMsg string) {
	writeStdJSONResp(w, http.StatusNotFound, JSONStdResponse{
		Error:  errMsg,
		Result: "ERROR",
	})
}

//writeStdJSONErrForbidden erreur applicative
func writeStdJSONErrForbidden(w http.ResponseWriter, errMsg string) {
	writeStdJSONResp(w, http.StatusForbidden, JSONStdResponse{
		Error:  errMsg,
		Result: "ERROR",
	})
}

//writeStdJSONCreated réponse ok std
func writeStdJSONCreated(w http.ResponseWriter, id string) {
	writeStdJSONResp(w, http.StatusCreated, JSONStdResponse{
		Error:  "",
		ID:     id,
		Result: "OK",
	})
}

//writeStdJSONAccepted réponse ok std
func writeStdJSONAccepted(w http.ResponseWriter, id string) {
	writeStdJSONResp(w, http.StatusAccepted, JSONStdResponse{
		Error:  "",
		ID:     id,
		Result: "OK",
	})
}

//writeStdJSONOK réponse ok std
func writeStdJSONOK(w http.ResponseWriter) {
	writeStdJSONResp(w, http.StatusOK, JSONStdResponse{
		Error:  "",
		ID:     "",
		Result: "OK",
	})
}

//panicHandler cas des requetes qui léverait un panic (evite que le program crash)
func panicHandler(w http.ResponseWriter, r *http.Request, err interface{}) {
	http.Error(w, fmt.Sprintln("Error", err), http.StatusInternalServerError)
	log.Println("Panic :", err)
}

//ping handler test connectivité
func ping(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Fprintf(w, "pong")
}
