package schd

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	agentCheckPeriod = time.Duration(2) * time.Second
)

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

// proceedTaskFlow execute le task flows
// et la tache en cours d'exec devrait pouvoir notifier chacune leur avancement)
func (c *PreparedTF) proceedTaskFlow(feedback chan<- wipInfo) {
	c.StartAt = time.Now()
	transcript := make([]string, 0)

	nextIdxToExec := 1 //idx commence à 1 en bdd

	for nextIdxToExec > 0 {
		transcript = append(transcript, fmt.Sprintf("Start idx %v : %v", nextIdxToExec, c.Detail[nextIdxToExec-1].Task.Lib))
		nextIdxB0 := nextIdxToExec - 1 //(index tableau = index bdd-1)

		if nextIdxB0 > (len(c.Detail) - 1) {
			transcript = append(transcript, fmt.Sprintf("Invalid next idx : %v, taskflow aborted", nextIdxToExec))
			nextIdxToExec = -1 //0: terminé ok, -1: terminé avec erreur
		} else {
			iTryCpt := c.Detail[nextIdxB0].RetryIfFail + 1 //nombre deressai + essai initiale

			//appel ws et gestion réponse
			waitFor := true
			exec := false
			var currentExecErr error

			for waitFor {
				//appel demande exec
				if !exec {
					execErr := c.Detail[nextIdxB0].agentQueryExec(c)
					if execErr != nil {
						currentExecErr = fmt.Errorf("error query agent : %v", execErr)
						break
					}
					exec = true
				}

				//attente et interro
				time.Sleep(agentCheckPeriod)
				tr, aerr := c.Detail[nextIdxB0].agentQueryState(c)
				if aerr != nil {
					currentExecErr = fmt.Errorf("error query state : %v", aerr)
					break
				}

				//selon état remonté par le ws
				if !tr.OnRegister {
					//tache inconnu de l'agent ?
					currentExecErr = fmt.Errorf("error query state : unknown sid %v", c.Detail[nextIdxB0].AgentSID)
					break
				} else if tr.Terminated {
					//tache terminé
					iTryCpt--
					if tr.ResOK {
						transcript = append(transcript, fmt.Sprintf("Task idx %v terminated, duration : %v", nextIdxToExec, tr.Duration))
						break
					} else {
						transcript = append(transcript, fmt.Sprintf("Task idx %v failed, duration : %v", nextIdxToExec, tr.Duration))
						if iTryCpt > 0 {
							//nouvel essai
							waitFor = true
							exec = false
						} else {
							currentExecErr = fmt.Errorf("task fail : %v", tr.ResInfo)
							break
						}
					}
				}
				// todo : else délai d'attente expiré?, demande de kill ?
				//maxDuration := time.Duration(c.Detail[nextIdxB0].Task.Timeout) * time.Second
				//if (maxDuration>0) && (time.Since(tstart) > maxDuration) {
				//}
			} // for wait for tache

			//suivant...
			if currentExecErr == nil {
				nextIdxToExec = c.Detail[nextIdxB0].NextTaskIDOK
			} else {
				transcript = append(transcript, currentExecErr.Error())
				nextIdxToExec = c.Detail[nextIdxB0].NextTaskIDFail
			}
		}

	}
	if nextIdxToExec == 0 {
		c.Result = 1
	} else {
		c.Result = -1
	}
	c.ResultMsg = strings.Join(transcript, "\n")
	c.StopAt = time.Now()
}

// calcArgs calcule les args de la tache en prenant en compte les eventuels arguments nommés de la tf
func (c *PreparedDetail) calcArgs(tf *PreparedTF) []string {
	out := make([]string, 0)
	for _, a := range c.Task.Args {
		for tag, val := range tf.NamedArgs {
			a = strings.ReplaceAll(a, "<%"+tag+"%>", val)
		}
		out = append(out, a)
	}
	return out
}

//agentQueryExec execute a tache concerné
func (c *PreparedDetail) agentQueryExec(parent *PreparedTF) error {
	var err error

	var tf *TaskView
	if c.Task.Type == "CmdTask" {
		tf = &TaskView{
			Type:    c.Task.Type,
			Timeout: int64(c.Task.Timeout),
			LogCfg:  c.Task.LogStore,
			Cmd:     c.Task.Cmd,
			Args:    c.calcArgs(parent),
			StartIn: c.Task.StartIn,
		}
	} else if c.Task.Type == "URLCheckTask" {
		tf = &TaskView{
			Type:    c.Task.Type,
			Timeout: int64(c.Task.Timeout),
			LogCfg:  c.Task.LogStore,
			URL:     c.Task.Cmd,
		}
	}

	if tf == nil {
		return fmt.Errorf("invalid task type")
	}

	//custom client pour l'eventuel skip de ctrl certifcat
	///TODO : les cert auto signé devrait validé sur labase de leur signature
	/// GetCertificate permettrait ça ?
	insClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: (c.Agent.CertSignAllowed != ""),
				//GetCertificate: ,
			},
		},
		Timeout: time.Duration(5) * time.Second,
	}

	b, err := json.Marshal(tf)
	if err != nil {
		return fmt.Errorf("error marshall tf %w", err)
	}

	url := c.Agent.Host + "/task/queue"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	req.Header.Add("X-Api-Key", c.Agent.APIKey)

	//appel avec reessai
	var resp *http.Response
	iMaxTry := 3
	for iTry := 0; iTry < iMaxTry; iTry++ {
		if iTry > 0 {
			time.Sleep(time.Second)
		}
		resp, err = insClient.Do(req)
		if err != nil && iTry == (iMaxTry-1) {
			return fmt.Errorf("agent call fail %w", err)
		} else if err == nil {
			break
		}
	}
	defer resp.Body.Close()

	//retour attendu : 202 Accepted avec id en corp json
	var aresp TaskReponse
	rb, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(rb, &aresp)
	if resp.StatusCode != 202 {
		return fmt.Errorf("agent return code = %v %v", resp.StatusCode, aresp.ErrMessage)
	} else if aresp.ID <= 0 {
		return fmt.Errorf("agent return invalid execution id")
	}
	c.AgentSID = int(aresp.ID)
	return nil
}

//agentQueryState interroge l'agent sur l'avancement
func (c *PreparedDetail) agentQueryState(parent *PreparedTF) (TaskReponse, error) {
	var err error
	var aresp TaskReponse

	if c.AgentSID <= 0 {
		return aresp, fmt.Errorf("invalid agent sid")
	}

	//custom client pour l'eventuel skip de ctrl certifcat
	///TODO : les cert auto signé devrait validé sur labase de leur signature
	/// GetCertificate permettrait ça ?
	insClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: (c.Agent.CertSignAllowed != ""),
				//GetCertificate: ,
			},
		},
		Timeout: time.Duration(2) * time.Second,
	}

	url := c.Agent.Host + "/task/queue/" + strconv.FormatInt(int64(c.AgentSID), 10)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return aresp, err
	}
	req.Header.Add("X-Api-Key", c.Agent.APIKey)

	//appel avec reessai
	var resp *http.Response
	iMaxTry := 5
	for iTry := 0; iTry < iMaxTry; iTry++ {
		if iTry > 0 {
			time.Sleep(time.Second)
		}
		resp, err = insClient.Do(req)
		if err != nil && iTry == (iMaxTry-1) {
			return aresp, fmt.Errorf("agent call fail %w", err)
		} else if err == nil {
			break
		}
	}
	defer resp.Body.Close()

	//retour attendu : 200 avec corp json
	rb, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(rb, &aresp)
	if resp.StatusCode != 200 && aresp.ErrMessage == "" {
		aresp.ErrMessage = fmt.Sprintf("agent response : %v", resp.StatusCode)
	}
	return aresp, nil
}
