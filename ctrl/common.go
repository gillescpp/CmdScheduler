package ctrl

import (
	"CmdScheduler/dal"
	"CmdScheduler/sessions"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/spf13/viper"
)

//variable globale controleur
var (
	SessionKey []byte //clé de cryptage cookie
)

//JSONStdResponse réponse json générique
type JSONStdResponse struct {
	Error  string `json:"errorMessage,omitempty"`
	ID     string `json:"id,omitempty"`
	Result string `json:"result,omitempty"`
}

// getBearerToken recup token bearer
func getBearerToken(r *http.Request) string {
	hAuth := r.Header.Get("Authorization")
	hs := strings.Split(hAuth, "Bearer ")
	if len(hs) == 2 {
		return strings.TrimSpace(hs[1])
	}
	return ""
}

// secMiddleWare middleware pour la gestion de la sécurité
func secMiddleWare(crudCode string, cors bool, next httprouter.Handle) httprouter.Handle {
	////TODO : si pas loggué : redirection, si ko : 503
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		// adjonction cors
		if cors {
			c := viper.GetString("allow-origin")
			if c != "" {
				w.Header().Set("Access-Control-Allow-Origin", c)
				if r.Method == "OPTIONS" && r.Header.Get("Access-Control-Request-Method") != "" { //query cors preflight
					w.Header().Set("Access-Control-Allow-Methods", "POST, PUT, GET, OPTIONS, DELETE")
					w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
				}
			}
		}

		// auth middleware, check token
		if crudCode != "" {
			//extraction token passé en "Authorization: Bearer <TOKEN>"
			token := getBearerToken(r)

			//pas de token, ou token invalide -> auth required 401
			s := sessions.Get(token)
			if token == "" || s == nil {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			//check test role user, on déduit le fait que ce soit une tentative de modif/insertion/delete de part le verbe
			edit := r.Method == "POST" || r.Method == "PUT" || r.Method == "PATCH" || r.Method == "DELETE"
			if !dal.IsAutorised(dal.RightLevel(s.RightLevel), crudCode, edit) {
				w.WriteHeader(http.StatusForbidden)
				return
			}
		}

		if next != nil {
			next(w, r, ps)
		} else {
			w.WriteHeader(http.StatusOK)
		}

	}
}

// Helpers pour les réponses API

//writeStdJSONResp output json réponse std
func writeStdJSONResp(w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(payload)
}

//writeStdJSONCreated 201 created
func writeStdJSONCreated(w http.ResponseWriter, locationURL, ID string) {
	w.Header().Set("Location", locationURL+"/"+ID)
	writeStdJSONResp(w, http.StatusCreated, JSONStdResponse{
		Error:  "",
		ID:     ID,
		Result: "OK",
	})
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

//writeStdJSONUnauthorized erreur applicative
func writeStdJSONUnauthorized(w http.ResponseWriter, errMsg string) {
	writeStdJSONResp(w, http.StatusUnauthorized, JSONStdResponse{
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
