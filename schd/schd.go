package schd

import (
	"CmdScheduler/dal"
	"fmt"
	"sync"
	"time"
)

//DbSchedState structure support tache à executer
type DbSchedState struct {
	dal.DbSched

	NextExec   time.Time
	Running    bool
	Queued     int
	ValidError error
}

//tableau de l'état des taches actives
type schState struct {
	schedList map[int]*DbSchedState
	taskMutex sync.Mutex

	//chan de pilotage
	checkDbCh     chan int //maj from db tache x ou 0 pour tout
	stopRequestCh chan bool
	terminatedCh  chan bool
}

// instance scheduler
var appSched schState

//Stop démarra le scheduleur
func Stop() {
	if appSched.stopRequestCh != nil {
		//demande et attente
		appSched.stopRequestCh <- true
		<-appSched.terminatedCh
		fmt.Println("Sched terminated")
	}
}

//Start démarra le scheduleur
func Start() {
	//init sched
	fmt.Println("Sched starting")
	Stop()

	appSched.schedList = make(map[int]*DbSchedState)
	appSched.checkDbCh = make(chan int)
	appSched.stopRequestCh = make(chan bool)
	appSched.terminatedCh = make(chan bool)

	//boucle de travail
	cpt := 0
	for {
		select {
		case schedID := <-appSched.checkDbCh:
			//maj liste taches
			updateSchedFromDb(schedID)

		case <-time.After(time.Second):
			//maj liste taches complete
			if cpt > 9 {
				cpt = 0
				updateSchedFromDb(0)
			}
			//traitement taches
			//proceedSched()

		case <-appSched.stopRequestCh:
			//arret du scheduleur
			fmt.Println("Sched stop request")
			//debloque le stop en attente
			close(appSched.terminatedCh)
			return
		}
	}
}

//UpdateSchedFromDb programme une maj de tache
func UpdateSchedFromDb(schedID int) {
	if appSched.checkDbCh != nil {
		appSched.checkDbCh <- schedID
	}
}

//updateSchedFromDb met à jour une tache depuis la bdd
func updateSchedFromDb(schedID int) {
	//recup état de la planning à mettre à jour (tout si schedID==0)
	q := dal.SearchQuery{
		Limit:  2000,
		Offset: 0,
	}
	if schedID > 0 {
		q.SQLFilter = "id = ?"
		q.SQLParams = []interface{}{schedID}
	} else {
		q.SQLFilter = "(activ = 1 and deleted_at is null)"
	}

	arr, _, err := dal.SchedList(q)
	if err != nil {
		fmt.Println("Sched updateSchedFromDb : " + err.Error())
		return
	}
	appSched.taskMutex.Lock()
	defer appSched.taskMutex.Unlock()
	updated := make(map[int]bool)

	for _, e := range arr {
		//on ne garde que les taches actives
		updated[e.ID] = true
		if !e.Activ || e.Deleted {
			delete(appSched.schedList, e.ID)
		} else {
			vError := e.Validate(false) //tache invalide mis en mémoire mais non traitable
			appSched.schedList[e.ID] = &DbSchedState{
				DbSched: dal.DbSched{
					ID:         e.ID,
					Activ:      (vError == nil),
					TaskFlowID: e.TaskFlowID,
					ErrLevel:   e.ErrLevel,
					QueueID:    e.QueueID,
					LastStart:  e.LastStart,
					LastStop:   e.LastStop,
					LastResult: e.LastResult,
					LastMsg:    e.LastMsg,
					Detail:     make([]dal.DbSchedDetail, len(e.Detail)),
				},
				ValidError: vError,
			}
			copy(appSched.schedList[e.ID].Detail, e.Detail)
		}
	}

	//si maj full, on vire les flaggués
	if schedID == 0 {
		for k := range appSched.schedList {
			if updated[k] != true {
				delete(appSched.schedList, k)
			}
		}
	}
}

//proceedSched parcours la liste de chose à faire
func proceedSched(schedID int) {
	appSched.taskMutex.Lock()
	defer appSched.taskMutex.Unlock()

	dtRef := time.Now()

	for _, v := range appSched.schedList {
		if v.ValidError == nil && !v.Running { //tache non valide non traité
			if v.NextExec.Before(dtRef) {
				//date dépassé
				v.Running = true
			} else {

			}
		}
	}

	/*
		NextExec time.Time
		Running  bool
		ValidError error
	*/

}
