package agent

import (
	"crypto/tls"
	"net/http"
	"time"
)

const (
	AgentCheckPeriod  = time.Duration(2) * time.Second
	AgentQueryTimeout = time.Duration(2) * time.Second
)

var (
	normalTransport   = http.DefaultTransport
	insecureTransport = &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
)

// GetHttpTransport Pour les appels aux agents : http.Transport keeps a pool of open connections for reuse.
// donc les custom transport doivent être reutilisés, et non pas reinstencié à chaque requete
// On fourni donc une fabrique pour ça
//TODO : les cert auto signé devrait validé sur labase de leur signature
// TLSClientConfig.GetCertificate permettrait ça ?
func GetHttpTransport(insecure bool, cersignAllowed string) http.RoundTripper {
	insec := insecure || (cersignAllowed != "")
	if insec {
		return insecureTransport
	}
	return normalTransport

}

// DoHttpRequest wrapper http.Do selon les contraintes de secu du transport demandé
func DoHttpRequest(req *http.Request, timeout time.Duration, insecure bool, cersignAllowed string) (*http.Response, error) {
	httpCli := &http.Client{
		Transport: GetHttpTransport(insecure, cersignAllowed),
		Timeout:   timeout,
	}
	return httpCli.Do(req)
}

//TaskView (repris du source de l'agent) est le json pour demaander l'execution d'une tache
type TaskView struct {
	Type    string `json:"type"`      //type de tache
	Timeout int64  `json:"timeout"`   //délai d'exec max en millisecondes
	LogCfg  string `json:"log_store"` //config log à appliquer

	Cmd     string   `json:"cmd"`      //Tache type commande : - path appli a exec
	Args    []string `json:"args"`     // - et ses args
	StartIn string   `json:"start_in"` // - dossier de démarage

	URL string `json:"url"` //Tache type check url up : - url à  controler
}

//TaskReponse (repris du source de l'agent) est le résultat de l'execution
type TaskReponse struct {
	ID         int64  `json:"id"`                    //id tache
	OnRegister bool   `json:"on_register"`           //id de tache dont le résultat est encore connue (faux peut indiquer soit que l'id n'a jamais existé, soit qu'il a traité mais on n'a plus son résultat à dispo)
	Terminated bool   `json:"terminated"`            //tache connue comme terminé
	ResOK      bool   `json:"result"`                //résultat (ok ou ko)
	ResInfo    string `json:"result_info,omitempty"` //info resultat
	Duration   int64  `json:"duration"`              //durée d'execution en ms
	ErrMessage string `json:"message"`               //message fourni en cas d'erreur
}
