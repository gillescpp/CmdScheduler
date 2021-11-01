package schd

import (
	"CmdScheduler/dal"
	"CmdScheduler/slog"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	periodCalc = time.Minute * 30 // période pré calculé des prochains sched id à traiter
)

// instance globale du scheduler
var appSched scheduleurState

// WipView info taches en cours : info worker + taches à venir
type WipView struct {
	WState
	NextTask []TState `json:"next_task"`
}

// represente le plannficateur de tache : il garde en mémoire
// tous les elements pour la lancement de taches : définition
// de celles-ci et plannfication en place
// pumpSched sert de boucle principale :
// * chargement/mise à jour des elements en mémoire (maj sur notif notifié par les controleur de l'api)
// * lancement des taches associé à schedid dont l'heure calculé est atteinte
// * chan start/stop
type scheduleurState struct {
	memMutex sync.Mutex

	//objet gerant la file d'execution piloté par la plannif
	worker *Worker

	//config en cours en mémoire
	schedLst     map[int]*dal.DbSched    // plannifs pilotant les exec
	agentsLst    map[int]*dal.DbAgent    // liste des agents
	queueLst     map[int]*dal.DbQueue    // liste des queue
	tasksLst     map[int]*dal.DbTask     // liste des taches
	taskflowsLst map[int]*dal.DbTaskFlow // liste des workflow
	schedToTF    map[int][]int           // lien schedid = liste des taches actives à lancer liés

	//prochain lancement calculé
	schdFrom        time.Time //date d'origine
	checkTick       *time.Ticker
	nextLaunchs     *nextExec
	nextRefreshCalc time.Time //date de rajout de calcules/rajout des prochaines dates
	lastNextTask    []TState  //info prochaines taches à venir pour consultations

	//chan de pilotage
	checkDbCh      chan chNotifyChange //demande maj from db tache x ou 0 pour tout
	instantStartTf chan int            //demande démarrage immédiat d'une tache
	stopRequestCh  chan bool           //chan demande arret (appel stop)
	terminatedCh   chan bool           //chan acquitement arret
}

// chNotifyChange typage notif changement de donnée
type chNotifyChange struct {
	dType string
	ID    int
}

// nextExec object pour gerer la liste des sched id à lancer à un instant t
type nextExec struct {
	grpSchedsToLaunch map[time.Time][]int //map date = liste des schedid concerné par cette date
}

// nextExec création nextExec
func newNextExec() *nextExec {
	return &nextExec{
		grpSchedsToLaunch: map[time.Time][]int{},
	}
}

// ajout d'un schedid a executer à l'heure t
func (c *nextExec) add(t time.Time, s int) {
	if t.IsZero() {
		return
	}

	exists := false
	if c.grpSchedsToLaunch[t] == nil {
		c.grpSchedsToLaunch[t] = make([]int, 0)
	} else {
		for _, scid := range c.grpSchedsToLaunch[t] {
			if scid == s {
				exists = true
				break
			}
		}
	}
	if !exists {
		c.grpSchedsToLaunch[t] = append(c.grpSchedsToLaunch[t], s)
	}
}

// popSchedIdBefore retourne et supprime les sched id a traiter (dt <= t) avec la date programmé
func (c *nextExec) popSchedIdBefore(t time.Time) map[int]time.Time {
	var mpSchedId map[int]time.Time
	for k := range c.grpSchedsToLaunch {
		//on revoie les chedid de toutes les dates dépassé dédoublonnée
		if k.Equal(t) || k.Before(t) {
			if mpSchedId == nil {
				mpSchedId = make(map[int]time.Time)
			}
			for _, scid := range c.grpSchedsToLaunch[k] {
				mpSchedId[scid] = k
			}
			//suppression elements traité
			delete(c.grpSchedsToLaunch, k)
		}
	}
	return mpSchedId
}

// popSchedIdAfter retourne et supprime les sched id a traiter dont dt > t
func (c *nextExec) popSchedIdAfter(t time.Time) map[int]bool {
	var mpSchedId map[int]bool
	for k := range c.grpSchedsToLaunch {
		//on revoie les chedid de toutes les dates dépassé dédoublonnée
		if k.After(t) {
			if mpSchedId == nil {
				mpSchedId = make(map[int]bool)
			}
			for _, scid := range c.grpSchedsToLaunch[k] {
				mpSchedId[scid] = true
			}
			//suppression elements traité
			delete(c.grpSchedsToLaunch, k)
		}
	}
	return mpSchedId
}

//Stop démarre le scheduleur
func Stop() {
	if appSched.stopRequestCh != nil {
		//demande et attente acq arret
		appSched.stopRequestCh <- true
		<-appSched.terminatedCh
		slog.Trace("sched", "Scheduler terminated")
	}
}

//Start démarre le scheduleur
func Start() {
	Stop()
	slog.Trace("sched", "Scheduler starting")
	//lancement traitement
	go pumpSched()
}

//UpdateSchedFromDb permet de notifier le scheduleur d'une modifs des données
func UpdateSchedFromDb(typeName string, ID int) {
	if appSched.checkDbCh != nil {
		appSched.checkDbCh <- chNotifyChange{
			dType: typeName,
			ID:    ID,
		}
	}
}

//GetViewState état pour dashboard
func GetViewState() WipView {
	var state WipView

	//état des queues & taches
	if appSched.worker != nil {
		state.WState = appSched.worker.GetLastState()
	}

	// prochaines tache à exec
	state.NextTask = appSched.lastNextTask
	return state
}

//calcViewNextState calcul un état des prochaines exec à à mener pour affichage
func calcViewNextState() {
	// prochaines tache à exec : il faut trier par date
	appSched.lastNextTask = make([]TState, 0)
	dtList := make([]time.Time, 0)
	for dt := range appSched.nextLaunchs.grpSchedsToLaunch {
		dtList = append(dtList, dt)
	}
	sort.Slice(dtList, func(i, j int) bool { return dtList[i].Before(dtList[j]) })

	//puis les dates sont associés à ou plusieurs schedid, lié à un ou plusieurs tf
	for idt := 0; (idt < len(dtList)) && (len(appSched.lastNextTask) < 30); idt++ { //liste 30 max pour le dash
		if arr, exists := appSched.nextLaunchs.grpSchedsToLaunch[dtList[idt]]; exists {
			for _, schedid := range arr {
				for _, tfidx := range appSched.schedToTF[schedid] {
					qlib := ""
					tf := appSched.taskflowsLst[tfidx]
					if _, exists := appSched.queueLst[tf.QueueID]; exists {
						qlib = appSched.queueLst[tf.QueueID].Lib
					}

					appSched.lastNextTask = append(appSched.lastNextTask, TState{
						TFID:     tf.ID,
						TFLib:    tf.Lib,
						QueueID:  tf.QueueID,
						QueueLib: qlib,
						Success:  false,
						DtRef:    dtList[idt],
					})
				}
			}
		}
	}
}

// pumpSched est la boucle principale de gestion des taches
func pumpSched() {
	// init données requises pour la gestion en mémoire
	appSched.schedLst = make(map[int]*dal.DbSched)
	appSched.agentsLst = make(map[int]*dal.DbAgent)
	appSched.queueLst = make(map[int]*dal.DbQueue)
	appSched.tasksLst = make(map[int]*dal.DbTask)
	appSched.taskflowsLst = make(map[int]*dal.DbTaskFlow)

	appSched.checkDbCh = make(chan chNotifyChange, 10)
	appSched.instantStartTf = make(chan int)
	appSched.stopRequestCh = make(chan bool)
	appSched.terminatedCh = make(chan bool)

	appSched.nextLaunchs = newNextExec()
	appSched.schdFrom = time.Now()
	appSched.lastNextTask = make([]TState, 0)

	// init entités en mémoire
	updateEntitiesFromDb("*", 0)

	//init worker
	appSched.worker = NewWorker(appSched.queueLst)
	appSched.worker.Start()

	//1er calcul plannif
	calcNextLaunch()

	//ticker pour les check régulier des prochaines taches a lancer
	appSched.checkTick = time.NewTicker(time.Second)
	slog.Trace("sched", "Scheduler ready")

	//boucle de travail :
	for {
		select {
		case e := <-appSched.checkDbCh:
			//traitement notif de modif des données
			updateEntitiesFromDb(e.dType, e.ID)
			//recalc sched si modifié
			if e.dType == "DbSched" {
				calcNextLaunch()
			} else if e.dType == "DbQueue" {
				//changement état qu'une queue peut affecter le worker (état pause)
				for _, q := range appSched.queueLst {
					appSched.worker.UpdateQueue(*q)
				}
			}
		case <-appSched.stopRequestCh:
			//arret du scheduleur
			slog.Trace("sched", "Scheduler stopping...")
			appSched.checkTick.Stop()
			//arret des traitement en cours, avec période de grace de 6s
			for i := 0; i < 30 && appSched.worker.Activ(); i++ {
				time.Sleep(time.Millisecond * 200)
			}

			//debloque le stop en attente
			close(appSched.terminatedCh)
			return

		case ct := <-appSched.checkTick.C:
			appSched.memMutex.Lock()
			mpSchedId := appSched.nextLaunchs.popSchedIdBefore(ct)
			calcViewNextState()
			appSched.memMutex.Unlock()
			if len(mpSchedId) > 0 {
				//Lancement des taches planifiés associés
				for schedid, t := range mpSchedId {
					launchTFBySchedId(schedid, t)
				}
			}
			appSched.schdFrom = ct
			if ct.After(appSched.nextRefreshCalc) {
				// maintient liste des plannifs fournies
				calcNextLaunch()
			}
		}
	}
}

//updateEntitiesFromDb maj entites bdd en mémoire
func updateEntitiesFromDb(entName string, id int) error {
	appSched.memMutex.Lock()
	defer func() {
		appSched.memMutex.Unlock()
	}()

	//taches
	if (entName == "*") || (entName == "DbTask") {
		f := dal.SearchQuery{
			Limit:  0,
			Offset: 0,
		}
		if id > 0 {
			f.SQLFilter = "TASK.id = ?"
			f.SQLParams = []interface{}{id}
		}
		updated := make(map[int]bool)
		resp, _, err := dal.TaskList(f)
		if err != nil {
			return fmt.Errorf("updateEntitiesFromDb DbTask : " + err.Error())
		}
		//maj tableau
		for e := range resp {
			appSched.tasksLst[resp[e].ID] = &resp[e]
			updated[resp[e].ID] = true
		}
		//suppression des elements obsoletes
		if id == 0 {
			for _, e := range appSched.tasksLst {
				if _, exists := updated[e.ID]; !exists {
					delete(appSched.tasksLst, e.ID)
				}
			}
		} else if _, exists := updated[id]; !exists {
			delete(appSched.tasksLst, id)
		}
	}
	//taskflows
	if (entName == "*") || (entName == "DbTaskFlow") {
		f := dal.SearchQuery{
			Limit:  0,
			Offset: 0,
		}
		if id > 0 {
			f.SQLFilter = "TASKFLOW.id = ?"
			f.SQLParams = []interface{}{id}
		}
		updated := make(map[int]bool)
		resp, _, err := dal.TaskFlowList(f)
		if err != nil {
			return fmt.Errorf("updateEntitiesFromDb DbTaskFlow : " + err.Error())
		}
		//maj tableau
		for e := range resp {
			appSched.taskflowsLst[resp[e].ID] = &resp[e]
			updated[resp[e].ID] = true
		}
		//suppression des elements obsoletes
		if id == 0 {
			for _, e := range appSched.taskflowsLst {
				if _, exists := updated[e.ID]; !exists {
					delete(appSched.taskflowsLst, e.ID)
				}
			}
		} else if _, exists := updated[id]; !exists {
			delete(appSched.taskflowsLst, id)
		}
		//on établie un lien sched id = liste de TF concerné
		appSched.schedToTF = make(map[int][]int)
		for idx, tf := range appSched.taskflowsLst {
			if tf.Activ && tf.ScheduleID > 0 {
				if appSched.schedToTF[tf.ScheduleID] == nil {
					appSched.schedToTF[tf.ScheduleID] = make([]int, 0)
				}
				appSched.schedToTF[tf.ScheduleID] = append(appSched.schedToTF[tf.ScheduleID], idx)
			}
		}
	}
	//agent
	if (entName == "*") || (entName == "DbAgent") {
		f := dal.SearchQuery{
			Offset: 0,
			Limit:  0,
		}
		if id > 0 {
			f.SQLFilter = "AGENT.id = ?"
			f.SQLParams = []interface{}{id}
		}
		updated := make(map[int]bool)
		resp, _, err := dal.AgentList(f)
		if err != nil {
			return fmt.Errorf("updateEntitiesFromDb DbAgent : " + err.Error())
		}
		//maj tableau
		for e := range resp {
			appSched.agentsLst[resp[e].ID] = &resp[e]
			updated[resp[e].ID] = true
		}
		//suppression des elements obsoletes
		if id == 0 {
			for _, e := range appSched.agentsLst {
				if _, exists := updated[e.ID]; !exists {
					delete(appSched.agentsLst, e.ID)
				}
			}
		} else if _, exists := updated[id]; !exists {
			delete(appSched.agentsLst, id)
		}
	}
	//queues
	if (entName == "*") || (entName == "DbQueue") {
		f := dal.SearchQuery{
			Limit:  0,
			Offset: 0,
		}
		if id > 0 {
			f.SQLFilter = "QUEUE.id = ?"
			f.SQLParams = []interface{}{id}
		}
		updated := make(map[int]bool)
		resp, _, err := dal.QueueList(f)
		if err != nil {
			return fmt.Errorf("updateEntitiesFromDb DbAgent : " + err.Error())
		}
		//maj tableau
		for e := range resp {
			appSched.queueLst[resp[e].ID] = &resp[e]
			updated[resp[e].ID] = true
		}
		//suppression des elements obsoletes
		if id == 0 {
			for _, e := range appSched.queueLst {
				if _, exists := updated[e.ID]; !exists {
					delete(appSched.queueLst, e.ID)
				}
			}
		} else if _, exists := updated[id]; !exists {
			delete(appSched.queueLst, id)
		}
	}
	//sched
	if (entName == "*") || (entName == "DbSched") {
		f := dal.SearchQuery{
			Limit:     0,
			Offset:    0,
			SQLFilter: "PERIOD.type = 1", //type sched seulement
		}
		if id > 0 {
			f.SQLFilter += " AND PERIOD.id = ?"
			f.SQLParams = []interface{}{id}
		}
		updated := make(map[int]bool)
		resp, _, err := dal.SchedList(f)
		if err != nil {
			return fmt.Errorf("updateEntitiesFromDb DbSched : " + err.Error())
		}
		//maj tableau
		for e := range resp {
			appSched.schedLst[resp[e].ID] = &resp[e]
			updated[resp[e].ID] = true
		}
		//suppression des elements obsoletes
		if id == 0 {
			for _, e := range appSched.schedLst {
				if _, exists := updated[e.ID]; !exists {
					delete(appSched.schedLst, e.ID)
				}
			}
		} else if _, exists := updated[id]; !exists {
			delete(appSched.schedLst, id)
		}
	}
	return nil
}

//calcNextLaunch calcul des prochaines dates de démarrage
func calcNextLaunch() {
	appSched.memMutex.Lock()
	defer func() {
		appSched.memMutex.Unlock()
	}()

	//ras des dates futures existantes qu'on va recalculer
	appSched.nextLaunchs.popSchedIdAfter(appSched.schdFrom)

	// on calcule les prochaines dates sur une periode a venir donnée
	calcTo := appSched.schdFrom.Add(periodCalc)
	// date à partir de laquelle rajouter des nouvelle dates afin d'avoir
	//toujours des dates à venir en visu
	appSched.nextRefreshCalc = appSched.schdFrom.Add(periodCalc / 2)
	for s := range appSched.schedLst {
		calcnext := true
		tmpfrom := appSched.schdFrom
		for calcnext {
			next := appSched.schedLst[s].CalcNextLaunch(tmpfrom) // ne renvoie rien si > 1 an
			if !next.IsZero() {
				if next.After(calcTo) {
					calcnext = false
				} else {
					tmpfrom = next
					appSched.nextLaunchs.add(next, s)
				}
			} else {
				calcnext = false
			}
		}
	}

	//maj valeurs calculés
	calcViewNextState()
}

//launchTFBySchedId lancement tache active associé à un schedid
func launchTFBySchedId(schedid int, dtRef time.Time) {
	for _, tf := range appSched.schedToTF[schedid] {
		ptf := prepareTF(appSched.taskflowsLst[tf], fmt.Sprintf("Schedule ID %v", schedid), dtRef, false)
		slog.Trace("sched", "Scheduler %v, push taskflow %v", schedid, ptf.Ident)
		appSched.worker.AppendTF(*ptf)
	}
}

//ManualLaunchTF lancement tache depuis api
func ManualLaunchTF(tfID int, usr string) {
	if _, exists := appSched.taskflowsLst[tfID]; exists {
		ptf := prepareTF(appSched.taskflowsLst[tfID], fmt.Sprintf("Manual launch by %v", usr), time.Now(), true)
		slog.Trace("api", "push taskflow %v by %v", ptf.Ident, usr)
		appSched.worker.AppendTF(*ptf)
	}
}
