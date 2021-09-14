package ctrl

import (
	"CmdScheduler/dal"
	"CmdScheduler/sessions"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

type JSONTokenResp struct {
	Token  string                   `json:"token,omitempty"`
	Rights map[string]dal.RightView `json:"rights,omitempty"`
}

//apiAuth retourne un token api (serait a remplacer pa du oauth)
func apiAuth(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//recup auth basic
	login, pass, ok := r.BasicAuth()
	if !ok || login == "" {
		writeStdJSONErrBadRequest(w, "bad request")
		return
	}

	//check auth
	usr, errAuth := dal.UserCheckAuth(login, pass)
	if usr.ID <= 0 || usr.Deleted || errAuth != nil {
		writeStdJSONUnauthorized(w, "invalid credential")
		return
	}

	//inib de l'eventuel session fourni à remplacer
	bt := getBearerToken(r)
	if bt != "" {
		sessions.Remove(bt)
	}

	// init nouvelle session
	s := sessions.New()
	s.Login = usr.Login
	s.RightLevel = usr.RightLevel
	s.Data["USRID"] = usr.ID

	//on attache les droits à la réponse
	rl := dal.GetRigthList(dal.RightLevel(s.RightLevel))

	writeStdJSONResp(w, http.StatusOK, JSONTokenResp{
		Token:  s.SessionId,
		Rights: rl,
	})
}

//apiDisconnect supprime une session donnée
func apiDisconnect(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//inib de l'eventuel session fourni
	bt := getBearerToken(r)
	if bt != "" {
		sessions.Remove(bt)
	}

	writeStdJSONResp(w, http.StatusOK, JSONTokenResp{})
}

//apiGetRightList retourne la liste de droit applicable pour l'user en cours
func apiGetRightList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//deserial input
	var rl map[string]dal.RightView
	s := sessions.Get(getBearerToken(r))
	if s != nil {
		rl = dal.GetRigthList(dal.RightLevel(s.RightLevel))
	}
	writeStdJSONResp(w, http.StatusOK, rl)
}
