package schd

import (
	"CmdScheduler/dal"
	"CmdScheduler/slog"
	"container/list"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type WorkState int

const (
	StateUndefined = iota
	StateNew
	StateQueued
	StateInProgress
	StateTerminated
)

// wipInfo données chan retour d'info des tf en cours d'exec
//(pas d'info utilé pour l'instan)
type wipInfo struct {
}

// WipQueueView info taches en cours
type WipQueueView struct {
	ID         int    `json:"id"`
	Lib        string `json:"lib"`
	Paused     bool   `json:"paused"`
	Waiting    int    `json:"waiting"`
	Processing int    `json:"processing"`
	Size       int    `json:"size"`
}

// Represente la partie execution des taches
// Les taches sont preparé (prepareTF) par la planificateur, puis injecté
// dans la liste des taches à faire
// * lancement des taches dans l'ordre et selon les contraintes associés (queue)
// * check avancement auprés des agents
// * persistance résultat en bdd
// (todo : notif avancement pour eventuel info sse)
type Worker struct {
	tasklstMutex sync.Mutex

	abort bool //flag demande d'arret  todo rempl par chans demande arret et acqu
	on    bool //flag en marche

	wipMsgFlkow chan wipInfo //chan de feedback des taches en cours

	taskList *list.List             // liste des taches à traiter
	taskMP   map[string]*PreparedTF // ident unic, mise en file de doublon interdit

	queueState map[int]*QueueState //états des queues + directe en clé 0

	lastStateInfo  map[int]WipQueueView //informatif seulement
	lastStateCheck time.Time            //dernier maj liste
}

// NewWorker instance worker
func NewWorker() *Worker {
	return &Worker{
		taskMP:      make(map[string]*PreparedTF),
		taskList:    list.New(),
		abort:       false,
		on:          false,
		wipMsgFlkow: make(chan wipInfo, 10),
		queueState:  make(map[int]*QueueState),
	}
}

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

// start lancement d'un tache dans une routine distinct
func (c *PreparedTF) start(feedback chan<- wipInfo) {
	c.State = StateInProgress
	go func(feedback chan<- wipInfo) {
		c.proceedTaskFlow(feedback)
		c.State = StateTerminated
		//notif worker
		feedback <- wipInfo{}
	}(feedback)
}

//ProceedTF ajoute la TF à la liste des tache à executer
func (w *Worker) ProceedTF(tf *PreparedTF) {
	if tf == nil || tf.Ident == "" || w.taskList == nil {
		return
	}
	// tache déja en file ignoré
	if _, exists := w.taskMP[tf.Ident]; exists {
		slog.Warning("sched", "Push %v - %v skipped (already in list)", tf.TFLib, tf.Ident)
		return
	}

	//ajout à la liste
	w.tasklstMutex.Lock()
	defer func() {
		w.tasklstMutex.Unlock()
		//notif worker potentiellement bloquant si chan full
		//fait hors lock mutex pour eviter un deathlock avec la boucle de traitement
		w.wipMsgFlkow <- wipInfo{}
	}()

	//check si la queue concerné est pleine
	if tf.QueueID > 0 && tf.State == StateNew {
		if q, exists := w.queueState[tf.QueueID]; exists {
			if q.isFull() {
				info := fmt.Sprintf("Queue %v full", q.Name)
				tf.Result = -1
				tf.CantLaunch = info
				tf.ResultMsg = info
			}
		}
	}

	// tache déja qualifié comme non traitable : seul la persitance en db doit être faite
	if tf.Result != 0 {
		tf.State = StateTerminated
	} else {
		tf.State = StateNew
	}

	w.taskMP[tf.Ident] = tf
	w.taskList.PushBack(tf)
}

//UpdateList check liste forcé
func (w *Worker) UpdateList() {
	w.wipMsgFlkow <- wipInfo{}
}

// état d'un queue
type QueueState struct {
	Processing  int //tache en cours d'exec
	Paused      bool
	MaxSize     int
	Name        string
	dtUpdated   time.Time
	toLaunchCpt int

	tmpProcessing  int //variable temporaire
	tmpToLaunchCpt int
}

// isFull check si un queue est pleine
func (q *QueueState) isFull() bool {
	if q != nil && q.MaxSize > 0 {
		if (q.toLaunchCpt + q.Processing) >= q.MaxSize {
			return true
		}
	}
	return false
}

//
// pumpWork corp traitement de tache
func (w *Worker) pumpWork() {
	//init état des queues
	w.initQueueState()

	checkTick := time.NewTicker(time.Duration(5) * time.Second)

	//boucle de travail :
	w.on = true
	for !w.abort {
		select {
		case <-w.wipMsgFlkow: //changement avancement d'un tache notifié
			w.updateTaskList()
		case <-checkTick.C: //check régulier si pas d'activité (utile ?)
			if time.Since(w.lastStateCheck).Seconds() > 5 {
				w.updateTaskList()
			}
		}
	}
	w.on = false
}

//
// initQueueState init état des queues
func (w *Worker) initQueueState() {
	dtRef := time.Now()

	//queue 0  = directe
	if _, exists := w.queueState[0]; !exists {
		w.queueState[0] = &QueueState{
			Name: "[Direct]",
		}
	}

	for qid, qptr := range appSched.queueLst {
		//nouvelle queue
		if _, exists := w.queueState[qid]; !exists {
			w.queueState[qid] = &QueueState{}
			slog.Trace("sched", "Init queue %v", qid)
		}
		w.queueState[qid].dtUpdated = dtRef
		w.queueState[qid].Name = qptr.Lib
		w.queueState[qid].MaxSize = qptr.MaxSize
		//check mise en pause
		if w.queueState[qid].Paused != qptr.PausedManual {
			w.queueState[qid].Paused = qptr.PausedManual
			slog.Trace("sched", "Queue %v, pause = %v", qid, qptr.PausedManual)
		}
	}

	//suppression de queue n'existant plus, et reset des cpt temporaire
	for qid := range w.queueState {
		w.queueState[qid].tmpProcessing = 0
		w.queueState[qid].tmpToLaunchCpt = 0

		if qid != 0 && !w.queueState[qid].dtUpdated.Equal(dtRef) {
			slog.Trace("sched", "Queue %v deleted", qid)
			delete(w.queueState, qid)
		}
	}
}

//
// updateTaskList analyse met à jour à la liste des tache en cours
func (w *Worker) updateTaskList() {
	w.tasklstMutex.Lock()
	defer func() {
		w.tasklstMutex.Unlock()
	}()

	//1 : relevé état des queues (tache en attente ou en cours)
	w.initQueueState()

	for e := w.taskList.Front(); e != nil; e = e.Next() {
		tf := e.Value.(*PreparedTF)

		//si la queue a été supprimé, on détache les tf liés
		if tf.QueueID != 0 {
			if _, exists := w.queueState[tf.QueueID]; !exists {
				tf.QueueID = 0
				tf.QueueLib = ""
				if tf.State == StateQueued {
					tf.State = StateNew
				}
			}
		}

		if tf.State == StateInProgress { //(en cours ou terminé encore en queue)
			w.queueState[tf.QueueID].tmpProcessing++
		}
		if tf.QueueID != 0 && tf.State == StateNew {
			tf.State = StateQueued //toute nouvelle tache lié à queue est mise en queue
			slog.Trace("sched", "Queue %v, append %v", w.queueState[tf.QueueID].Name, tf.lib())
		}
	}

	// 2 : traitement avancement
	forceRefresh := false
	for e := w.taskList.Front(); e != nil; e = e.Next() {
		tf := e.Value.(*PreparedTF)

		if tf.State == StateNew {
			//tache à lancer, non lié à queue puisque en StateNew
			slog.Trace("sched", "Launching %v", tf.lib())
			tf.start(w.wipMsgFlkow)
			w.queueState[tf.QueueID].tmpProcessing++
		} else if tf.State == StateQueued {
			//check dispo queue
			if !w.queueState[tf.QueueID].Paused {
				//queue libre, on lance
				if w.queueState[tf.QueueID].tmpProcessing == 0 {
					w.queueState[tf.QueueID].tmpProcessing++
					slog.Trace("sched", "Queue %v, Launching %v", w.queueState[tf.QueueID].Name, tf.lib())
					tf.start(w.wipMsgFlkow)
				} else {
					// on prend note du fait qu'au moins 1 tache est en attente suite à celle en cours
					w.queueState[tf.QueueID].tmpToLaunchCpt++
				}
			} else {
				w.queueState[tf.QueueID].tmpToLaunchCpt++
			}
		}

		//tache terminé ou passé en terminé dans cette meme boucle
		if tf.State == StateTerminated {
			//tache terminé, on persiste en bdd et on supprime
			if tf.CantLaunch != "" {
				slog.Warning("sched", "Skipped %v : %v", tf.lib(), tf.CantLaunch)
			} else {
				if tf.QueueID != 0 {
					slog.Trace("sched", "Terminated %v : %v", w.queueState[tf.QueueID].Name, tf.lib())
					//une place s'est libéré
					forceRefresh = true
				} else {
					slog.Trace("sched", "Terminated %v", tf.lib())
				}
			}

			errDb := dal.TaskFlowUpdateLastState(tf.TFID, tf.StartAt, tf.StopAt, tf.Result, tf.ResultMsg)
			if errDb != nil {
				slog.Error("sched", "TaskFlowUpdateLastState fail %v", errDb)
			}
			w.taskList.Remove(e)
			delete(w.taskMP, tf.Ident)
		}
	}

	// bilan queue
	newStateView := make(map[int]WipQueueView)
	for qid := range w.queueState {
		w.queueState[qid].Processing = w.queueState[qid].tmpProcessing
		w.queueState[qid].toLaunchCpt = w.queueState[qid].tmpToLaunchCpt

		newStateView[qid] = WipQueueView{
			ID:         qid,
			Lib:        w.queueState[qid].Name,
			Paused:     w.queueState[qid].Paused,
			Waiting:    w.queueState[qid].toLaunchCpt,
			Processing: w.queueState[qid].Processing,
			Size:       w.queueState[qid].MaxSize,
		}
	}
	w.lastStateInfo = newStateView
	w.lastStateCheck = time.Now()

	// si une place s'est liberé dans un queue, on lance un nouveau check
	if forceRefresh {
		w.wipMsgFlkow <- wipInfo{}
	}
}
