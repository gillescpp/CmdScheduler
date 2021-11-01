package schd

import (
	"CmdScheduler/agent"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// proceedTaskFlow execute le task flows
// et la tache en cours d'exec devrait pouvoir notifier chacune leur avancement)
func (c *PreparedTF) proceedTaskFlow(feedback chan<- wipInfo) {
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
					//spec tache de test : pas d'action reel
					if c.Detail[nextIdxB0].Task.Type == "none" {
						time.Sleep(time.Duration(100+rand.Intn(900)) * time.Millisecond)
						transcript = append(transcript, fmt.Sprintf("Task idx %v terminated (TEST)", nextIdxToExec))
						break
					}
					//appel agent
					execErr := c.Detail[nextIdxB0].agentQueryExec(c)
					if execErr != nil {
						currentExecErr = fmt.Errorf("error query agent : %v", execErr)
						break
					}
					exec = true
				}

				//attente et interro
				time.Sleep(agent.AgentCheckPeriod)
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
				// todo : else délai d'attente expiré?
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

	var tf *agent.TaskView
	if c.Task.Type == "CmdTask" {
		tf = &agent.TaskView{
			Type:    c.Task.Type,
			Timeout: int64(c.Task.Timeout),
			LogCfg:  c.Task.LogStore,
			Cmd:     c.Task.Cmd,
			Args:    c.calcArgs(parent),
			StartIn: c.Task.StartIn,
		}
	} else if c.Task.Type == "URLCheckTask" {
		tf = &agent.TaskView{
			Type:    c.Task.Type,
			Timeout: int64(c.Task.Timeout),
			LogCfg:  c.Task.LogStore,
			URL:     c.Task.Cmd,
		}
	}

	if tf == nil {
		return fmt.Errorf("invalid task type")
	}

	// query agent...
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
		resp, err = agent.DoHttpRequest(req, agent.AgentQueryTimeout, false, c.Agent.CertSignAllowed)
		if err == nil {
			defer resp.Body.Close()
		}
		if err != nil && iTry == (iMaxTry-1) {
			return fmt.Errorf("agent call fail %w", err)
		} else if err == nil {
			break
		}
	}

	//retour attendu : 202 Accepted avec id en corp json
	var aresp agent.TaskReponse
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
func (c *PreparedDetail) agentQueryState(parent *PreparedTF) (agent.TaskReponse, error) {
	var err error
	var aresp agent.TaskReponse

	if c.AgentSID <= 0 {
		return aresp, fmt.Errorf("invalid agent sid")
	}

	// query agent...
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
		resp, err = agent.DoHttpRequest(req, agent.AgentQueryTimeout, false, c.Agent.CertSignAllowed)
		if err == nil {
			defer resp.Body.Close()
		}
		if err != nil && iTry == (iMaxTry-1) {
			return aresp, fmt.Errorf("agent call fail %w", err)
		} else if err == nil {
			break
		}
	}

	//retour attendu : 200 avec corp json
	rb, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(rb, &aresp)
	if resp.StatusCode != 200 && aresp.ErrMessage == "" {
		aresp.ErrMessage = fmt.Sprintf("agent response : %v", resp.StatusCode)
	}
	return aresp, nil
}
