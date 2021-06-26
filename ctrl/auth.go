package ctrl

import (
	"CmdScheduler/dal"
	"CmdScheduler/sessions"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

type JSONTokenResp struct {
	Token string `json:"token,omitempty"`
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
	////TODO
	if !(login == "user" && pass == "pass") {
		writeStdJSONUnauthorized(w, "invalid credential")
		return
	}

	//init session
	s := sessions.New()
	s.Login = "user" //todo : depuis db
	s.Role = "ADMIN"
	s.RightLevel = 100
	writeStdJSONResp(w, http.StatusOK, JSONTokenResp{
		Token: s.SessionId,
	})
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
