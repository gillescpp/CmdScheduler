package ctrl

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

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

// Helpers pour les réponses API

//writeStdJSONResp output json réponse std
func writeStdJSONResp(w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(payload)
}

//writeStdJSONCreated 201 created
func writeStdJSONCreated(w http.ResponseWriter, locationURL string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Location", locationURL)
	w.WriteHeader(http.StatusCreated)
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
