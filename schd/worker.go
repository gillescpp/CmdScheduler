package schd

import (
	"CmdScheduler/dal"
	"CmdScheduler/slog"
	"container/list"
	"sort"
	"time"
)

const (
	// durée de rétention des taches terminés
	keepTerminatedTaskFor = 2 * time.Second
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
type wipInfo struct {
	tf       *PreparedTF
	newState WorkState
}

//TState info tache en cours
type TState struct {
	TFID  int    `json:"taskflow_id"`
	TFLib string `json:"taskflow_lib"`

	QueueID  int    `json:"queue_id"`
	QueueLib string `json:"queue_lib"`

	State int `json:"state"`

	LaunchSource string    `json:"launch_source"`
	Success      bool      `json:"success"`
	DtRef        time.Time `json:"dt_ref"`
	StartAt      time.Time `json:"start_at"`
	StopAt       time.Time `json:"stop_at"`
	Duration     string    `json:"duration"`
}

//WState info taches en cours
type WState struct {
	QueueState []qState `json:"queues"` //états des queues
	Tasks      []TState `json:"tasks"`  //états des taches
}

//qState info
type qState struct {
	dal.DbQueue

	Processing int `json:"processing"` // en cours d'execution
	Waiting    int `json:"waiting"`
	Launched   int `json:"launched"`   //total globale
	Terminated int `json:"terminated"` //total globale
}

//isFull() retourne vrai si la queue est full
func (s *qState) isFull() bool {
	return ((s.MaxSize > 0) && ((s.Processing + s.Waiting) >= s.MaxSize))
}

//canDoNewWork() retourne vrai si la queue peut prendre en charge une exec de tf
func (s *qState) canDoNewWork() bool {
	return (s.Processing < s.Slot) && !s.PausedManual
}

//Worker données du worker
//le worker travail seul sur ses données, toutes les com passe par des chans
type Worker struct {
	queueChan  chan dal.DbQueue //chan maj état d'un queue
	tfChan     chan PreparedTF  //chan tache à executer
	tfFeedback chan wipInfo     //chan tache à executer

	started bool
	actif   bool //état

	taskList *list.List             // liste des taches à traiter
	taskMP   map[string]*PreparedTF // ident unic, mise en file de doublon interdit

	queueState map[int]*qState //états des queues + directe en clé 0

	lastStateInfo *WState //informatif seulement, état des lieux taches en cours
}

//updateQStateFromQueue util maj état queue
func updateQStateFromQueue(dest *qState, from *dal.DbQueue) {
	if dest != nil && from != nil {
		dest.ID = from.ID
		dest.Lib = from.Lib
		dest.Slot = from.Slot
		dest.MaxSize = from.MaxSize
		dest.MaxDuration = from.MaxDuration
		dest.PausedManual = from.PausedManual
		dest.PausedManualFrom = from.PausedManualFrom
		dest.NoExecWhile = from.NoExecWhile
		dest.Info = from.Info
	}
}

// NewWorker instance worker avec
func NewWorker(initQueue map[int]*dal.DbQueue) *Worker {
	queueState := make(map[int]*qState)
	for _, q := range initQueue {
		queueState[q.ID] = &qState{}
		updateQStateFromQueue(queueState[q.ID], q)
	}
	//q 0 : direct
	queueState[0] = &qState{
		DbQueue: dal.DbQueue{
			ID:   0,
			Lib:  "[Direct]",
			Slot: 1000, //(theoriquement illimité)
		},
		Processing: 0,
		Waiting:    0,
		Launched:   0,
		Terminated: 0,
	}

	return &Worker{
		queueChan:  make(chan dal.DbQueue, 10),
		tfChan:     make(chan PreparedTF, 10),
		tfFeedback: make(chan wipInfo, 50),

		actif:   false,
		started: false,

		taskList: list.New(),
		taskMP:   make(map[string]*PreparedTF),

		queueState: queueState,

		lastStateInfo: &WState{},
	}
}

// Start lance le worker
func (c *Worker) Start() {
	if !c.actif {
		c.actif = true
		go c.loop()
	}
}

// Stop le worker
func (c *Worker) Stop() {
	c.actif = false
}

// Activ retourne l'état du worker
func (c *Worker) Activ() bool {
	return c.started
}

// GetLastState retourne le dernier éta des travaux en cours
func (c *Worker) GetLastState() WState {
	return *c.lastStateInfo
}

// UpdateQueue demande un maj d'un queue
func (c *Worker) UpdateQueue(q dal.DbQueue) {
	if c.actif && c.started {
		c.queueChan <- q
	}
}

// AppendTF demande un maj d'un queue
func (c *Worker) AppendTF(tf PreparedTF) {
	if c.actif {
		c.tfChan <- tf
	}
}

// loop boucle principale
func (c *Worker) loop() {
	c.started = true
	//1ere init dernier état connu
	c.checkTaskList()
	cleantick := time.NewTicker(1 * time.Second)

	for c.actif {
		checkTaskList := false
		select {
		case f := <-c.tfFeedback:
			//renforcement feed back sur routine worker
			checkTaskList = c.updTF(f)
		case q := <-c.queueChan:
			//notif d'un ajout/modif de queue
			checkTaskList = c.updateQueue(&q)
		case tf := <-c.tfChan:
			//arrivé d'un nouveau tf
			checkTaskList = c.appendTF(&tf)
		case <-cleantick.C:
			//netoayge régulier des taches
			c.cleanTasks("")
			c.calcState()
		}
		//maj taches
		if checkTaskList {
			c.checkTaskList()
		}
	}
	cleantick.Stop()

	c.started = false
}

// updateQueue met à jour l'état d'un queue
// on considére une queue supprimé si elle a 0 slot
func (c *Worker) updateQueue(q *dal.DbQueue) bool {
	if q != nil {
		if q.ID <= 0 {
			return false
		}
		if q.Slot == 0 {
			delete(c.queueState, q.ID)
			slog.Trace("worker", "Queue %v deleted", q.Lib)
			return true
		}
		if _, exists := c.queueState[q.ID]; !exists {
			c.queueState[q.ID] = &qState{}
			slog.Trace("worker", "New Queue %v", q.Lib)
		} else if c.queueState[q.ID].PausedManual != q.PausedManual {
			if q.PausedManual {
				slog.Trace("worker", "Queue %v paused", q.Lib)
			} else {
				slog.Trace("worker", "Queue %v pause terminated", q.Lib)
			}
		}
		updateQStateFromQueue(c.queueState[q.ID], q)
		return true
	}
	return false
}

// appendTF arrivé d'une nouvelle tache
func (c *Worker) appendTF(tf *PreparedTF) bool {
	if tf == nil || tf.Ident == "" {
		return false
	}

	// protections avant entrée en file
	// tache déja en file ignoré
	if _, exists := c.taskMP[tf.Ident]; exists {
		if c.taskMP[tf.Ident].State != StateTerminated {
			slog.Warning("worker", "Push %v - %v skipped (already in list)", tf.TFLib, tf.Ident)
			return false
		}
	}
	//si la queue est pleine, on skip
	if tf.QueueID > 0 {
		_, qexists := c.queueState[tf.QueueID]
		if !qexists {
			slog.Warning("worker", "Push %v skipped (%v not exists)", tf.lib(), tf.qlib())
			return false
		}

		c.initQueueState() //calcul état cumuls queue requis avant
		if c.queueState[tf.QueueID].isFull() {
			slog.Warning("worker", "Push %v skipped (%v is full)", tf.lib(), tf.qlib())
			return false
		}
	}

	//prise de proprité de la tf
	tf.State = StateNew
	c.cleanTasks(tf.Ident)
	c.taskMP[tf.Ident] = tf
	c.taskList.PushBack(tf)
	return true
}

// initQueueState réfencement état des queue
func (c *Worker) initQueueState() {
	//ras compteur
	for k := range c.queueState {
		c.queueState[k].Processing = 0
		c.queueState[k].Waiting = 0
	}

	//référencement état des taches
	for e := c.taskList.Front(); e != nil; e = e.Next() {
		tf := e.Value.(*PreparedTF)

		//si la queue a été supprimé, on détache les tf liés
		if tf.QueueID != 0 && (tf.State == StateNew || tf.State == StateQueued) {
			_, qexists := c.queueState[tf.QueueID]
			if !qexists {
				tf.QueueID = 0
				tf.QueueLib = ""
				if tf.State == StateQueued {
					tf.State = StateNew
				}
			} else {
				if tf.State == StateNew {
					tf.State = StateQueued
					slog.Trace("worker", "Queue %v, append %v |%v/%v|", tf.qlib(), tf.lib(),
						(c.queueState[tf.QueueID].Waiting+c.queueState[tf.QueueID].Processing)+1, c.queueState[tf.QueueID].MaxSize)
				}
			}
		}

		// maj compteur
		if tf.State == StateInProgress {
			c.queueState[tf.QueueID].Processing++
		} else if tf.State != StateTerminated {
			c.queueState[tf.QueueID].Waiting++
		}
	}
}

// launchTasks démarre les taches éligible à lancement
func (c *Worker) launchTasks() {
	for e := c.taskList.Front(); e != nil; e = e.Next() {
		tf := e.Value.(*PreparedTF)

		//tache soumise à queue à lancer
		if tf.State == StateQueued || tf.State == StateNew {
			if c.queueState[tf.QueueID].canDoNewWork() {
				//un slot es dispo, on lance
				tf.State = StateInProgress
				c.queueState[tf.QueueID].Processing++
				c.queueState[tf.QueueID].Waiting--
				c.queueState[tf.QueueID].Launched++

				slog.Trace("worker", "Launch %v : %v (P=%v / S=%v)", tf.qlib(), tf.lib(),
					c.queueState[tf.QueueID].Processing, c.queueState[tf.QueueID].Slot)

				tf.StartAt = time.Now()
				go func(feedback chan<- wipInfo) {
					tf.proceedTaskFlow(feedback)
					//notif loop fin de tache
					feedback <- wipInfo{
						tf:       tf,
						newState: StateTerminated,
					}
				}(c.tfFeedback)
			}
		}
	}
}

// cleanTasks supprime les taches terminé depuis un certains temps
func (c *Worker) cleanTasks(forceremove string) {
	for e := c.taskList.Front(); e != nil; e = e.Next() {
		tf := e.Value.(*PreparedTF)

		//tache soumise à queue à lancer
		if tf.State == StateTerminated {
			rm := !tf.StopAt.IsZero() && (time.Since(tf.StopAt) > keepTerminatedTaskFor)
			if rm || (tf.Ident == forceremove) {
				c.taskList.Remove(e)
				delete(c.taskMP, tf.Ident)
			}
		}
	}
}

// updTF maj tf sur arrivé d'un info de maj
// retourne vrai si la liste des taches doit être réévalué
func (c *Worker) updTF(f wipInfo) bool {
	if f.tf.State != f.newState {
		f.tf.State = f.newState
		//fin de tache : changement état et persitance db
		if f.tf.State == StateTerminated {
			f.tf.StopAt = time.Now()
			slog.Trace("worker", "End %v : %v", f.tf.qlib(), f.tf.lib())
			c.queueState[f.tf.QueueID].Terminated++
			//persitance db
			if f.tf.TFID != 0 {
				errDb := dal.TaskFlowUpdateLastState(f.tf.TFID, f.tf.StartAt, f.tf.StopAt, f.tf.Result, f.tf.ResultMsg)
				if errDb != nil {
					slog.Error("worker", "TaskFlowUpdateLastState fail %v", errDb)
				}
			}
			return true
		}
	}
	return false
}

// calcState bilan état des travaux en cours
func (c *Worker) calcState() {
	newStateInfo := WState{
		QueueState: make([]qState, len(c.queueState)),
		Tasks:      make([]TState, c.taskList.Len()),
	}

	//référencement état des taches
	it := 0
	for e := c.taskList.Front(); e != nil; e = e.Next() {
		tf := e.Value.(*PreparedTF)
		duration := time.Duration(0)
		if !tf.StartAt.IsZero() {
			if !tf.StopAt.IsZero() {
				duration = tf.StopAt.Sub(tf.StartAt)
			} else {
				duration = time.Now().Sub(tf.StartAt)
			}
		}

		newStateInfo.Tasks[it] = TState{
			TFID:         tf.TFID,
			TFLib:        tf.lib(),
			QueueID:      tf.QueueID,
			QueueLib:     tf.QueueLib,
			State:        int(tf.State),
			LaunchSource: tf.LaunchSource,
			Success:      (tf.Result == 1),
			DtRef:        tf.DtRef,
			StartAt:      tf.StartAt,
			StopAt:       tf.StopAt,
			Duration:     duration.String(),
		}
		it++
	}
	sort.Slice(newStateInfo.Tasks, func(i, j int) bool {
		return newStateInfo.Tasks[i].TFLib < newStateInfo.Tasks[j].TFLib
	})

	//et des queues
	it = 0
	for q := range c.queueState {
		newStateInfo.QueueState[it] = *c.queueState[q]
		it++
	}
	sort.Slice(newStateInfo.QueueState, func(i, j int) bool {
		return newStateInfo.QueueState[i].ID < newStateInfo.QueueState[j].ID
	})

	c.lastStateInfo = &newStateInfo
}

// checkTaskList mise à jour état de la liste
func (c *Worker) checkTaskList() {
	//init état des queue
	c.initQueueState()
	//et lancement des taches qui doivent l'être
	c.launchTasks()
	//netoyage tache plu utiles
	c.cleanTasks("")
	//puis maj dernier état connu
	c.calcState()
}
