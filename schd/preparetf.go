package schd

import (
	"CmdScheduler/dal"
	"fmt"
	"strconv"
	"strings"
	"time"
)

//PreparedDetail complément d'info pour le lancement d'un tache
type PreparedDetail struct {
	dal.DbTaskFlowDetail

	Agent    dal.DbAgent //inf agent finalement utilisé
	Task     dal.DbTask
	AgentSID int //id tache retourné par l'agent
}

//PreparedTF struct tf préparé
type PreparedTF struct {
	TFID      int
	TFLib     string
	Ident     string
	DtRef     time.Time
	Detail    []PreparedDetail
	NamedArgs map[string]string

	LaunchSource string //info source du démarrage

	ErrMngt  int
	QueueID  int
	QueueLib string

	StartAt    time.Time
	StopAt     time.Time
	Result     int
	ResultMsg  string //info persisté en db
	CantLaunch string //info tracé si lancement impossible

	State WorkState
}

//prepareTF prepa/qualif une taskflow avant lancement
func prepareTF(tf *dal.DbTaskFlow, launchInfo string, dtRef time.Time, manualLaunch bool) *PreparedTF {
	ptf := &PreparedTF{
		TFID:         tf.ID,
		TFLib:        tf.Lib,
		Ident:        "",
		DtRef:        dtRef,
		Detail:       make([]PreparedDetail, len(tf.Detail)),
		NamedArgs:    make(map[string]string),
		LaunchSource: launchInfo,
		ErrMngt:      tf.ErrMngt,
		QueueID:      tf.QueueID,
		QueueLib:     "",
		StartAt:      time.Time{},
		StopAt:       time.Time{},
		Result:       0,
		ResultMsg:    "",
		CantLaunch:   "",
		State:        StateUndefined,
	}

	//prepa argument nommée
	ident := "TF" + strconv.FormatInt(int64(ptf.TFID), 10)
	for k, v := range tf.NamedArgs {
		v = replaceArgsTags(v, ptf.DtRef)
		ptf.NamedArgs[k] = v
		ident += " [" + k + "=" + v + "]"
	}
	//l'ident servant de base pour la non execution simultannée d'une tache
	//est constitué de l'id de la TF est des arguments nommées
	//(si les arguments calculés donne une variation, cas d'un arg calculé avec date
	//, on considére que c'est deux tache distinct)
	ptf.Ident = ident

	//pré validation de certains composants
	cantLaunch := ""
	if manualLaunch && !tf.ManualLaunch {
		cantLaunch = "Manual launch is not allowed"
	}

	//controle queue
	if cantLaunch == "" && ptf.QueueID > 0 {
		if _, exists := appSched.queueLst[ptf.QueueID]; !exists {
			cantLaunch = fmt.Sprintf("Queue ID %v not found", ptf.QueueID)
		} else {
			ptf.QueueLib = appSched.queueLst[ptf.QueueID].Lib
		}
	}

	//controle existance des task id, et des agent cibles
	if cantLaunch == "" && len(ptf.Detail) == 0 {
		cantLaunch = "Empty taskflow"
	}
	if cantLaunch == "" {
		for i := range tf.Detail {
			//reprise des infos sur la source
			ptf.Detail[i].Idx = tf.Detail[i].Idx
			ptf.Detail[i].TaskID = tf.Detail[i].TaskID
			ptf.Detail[i].NextTaskIDOK = tf.Detail[i].NextTaskIDOK
			ptf.Detail[i].NextTaskIDFail = tf.Detail[i].NextTaskIDFail
			ptf.Detail[i].RetryIfFail = tf.Detail[i].RetryIfFail

			//def tache
			if _, exists := appSched.tasksLst[ptf.Detail[i].TaskID]; !exists {
				cantLaunch = fmt.Sprintf("Task ID %v not found", ptf.Detail[i].TaskID)
				break
			}
			ptf.Detail[i].Task = *appSched.tasksLst[ptf.Detail[i].TaskID]

			//check agent d'execution spécifié, récup du premier existant
			//todo : pourrait aussi s'appuyer sur un état des agents s'il existe un jour
			agent := 0
			for _, a := range appSched.tasksLst[ptf.Detail[i].TaskID].ExecOn {
				if _, exists := appSched.agentsLst[a]; exists {
					if !appSched.agentsLst[a].Deleted {
						agent = a
						break
					}
				}
			}
			if agent == 0 {
				cantLaunch = fmt.Sprintf("Task ID %v : agent not found", ptf.Detail[i].TaskID)
				break
			} else {
				ptf.Detail[i].Agent = *appSched.agentsLst[agent]
			}

		}
	}
	if cantLaunch != "" {
		ptf.Result = -1
		ptf.CantLaunch = cantLaunch
		ptf.ResultMsg = cantLaunch
	}
	return ptf
}

// replaceArgsTags replace les eventuels tags <%xxx%> dans l'argument
func replaceArgsTags(in string, dt time.Time) string {
	out := ""
	for in != "" {
		iTag := strings.Index(in, "<%")
		if iTag < 0 {
			//pas/plus de tag
			out += in
			in = ""
		} else {
			//recup elements pre tag
			out += in[0:iTag]
			in = in[iTag:]

			//traitement tag si présence balise de fin
			iTag2 := strings.Index(in, "%>")
			tag := ""
			if iTag2 >= 2 {
				tag = strings.ToUpper(strings.TrimSpace(in[2:iTag2]))
			}
			//traitement des tags gérés (tags non traité non remplacé)
			if (tag != "") && (tag[0:3] == "DT_") {
				valrp := ""
				//type de tag <%DT_xxx> ou contient un element de date
				// DD MM YYYY  HH NN SS
				if tag[0:3] == "DT_" {
					fmt := tag[3:]
					fmt = strings.ReplaceAll(fmt, "YYYY", "2006") //transposé au format de la lib std
					fmt = strings.ReplaceAll(fmt, "YY", "06")
					fmt = strings.ReplaceAll(fmt, "MM", "01")
					fmt = strings.ReplaceAll(fmt, "DD", "02")
					fmt = strings.ReplaceAll(fmt, "HH", "15")
					fmt = strings.ReplaceAll(fmt, "NN", "04")
					fmt = strings.ReplaceAll(fmt, "SS", "05")
					valrp = dt.Format(fmt)
				}

				out += valrp
				in = in[iTag2+2:]
			} else {
				out += in
				in = ""
			}
		}
	}
	return out
}

// lib util ident dans les logs
func (c *PreparedTF) lib() string {
	return c.TFLib + " - " + c.Ident
}

// lib util ident queue dans les logs
func (c *PreparedTF) qlib() string {
	if c.QueueID == 0 {
		return "[-]"
	}
	return "[" + c.QueueLib + "]"
}
