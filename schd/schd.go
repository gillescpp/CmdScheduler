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

	//copy queue et tache associé
	Queue    dal.DbQueue
	TaskFlow dal.DbTaskFlow

	//flag todo
	NeedDbUpdate bool

	NextExec   time.Time
	Running    bool
	Queued     bool
	ValidError error
}

//tableau de l'état des taches actives
type schState struct {
	taskMutex     sync.Mutex
	schedList     map[int]*DbSchedState
	schedListInit bool

	//chan de pilotage
	checkDbCh      chan chNotifyChange //demande maj from db tache x ou 0 pour tout
	instantStartTf chan int            //demande démarrage immédiat d'une tache
	stopRequestCh  chan bool
	terminatedCh   chan bool
}

// instance scheduler
var appSched schState

// chNotifyChange pour les notif de changement de donnée
type chNotifyChange struct {
	dType string
	ID    int
}

//pumpSched est la boucle principale de gestion des tache
func pumpSched() {

	appSched.schedList = make(map[int]*DbSchedState)
	appSched.checkDbCh = make(chan chNotifyChange, 10)
	appSched.instantStartTf = make(chan int)
	appSched.stopRequestCh = make(chan bool)
	appSched.terminatedCh = make(chan bool)

	// init taches à executer
	updateSchedFromDb(0)

	//boucle de travail :
	for {
		select {
		/*case e := <-appSched.checkDbCh:

		//flag de la tache comme à mettre à jour (action realisé par proceedSched quand taches en question dispo)
		if appSched.schedListInit {
			if e.dType == "DbSched" {
				//modif d'un sched
				if _, exists := appSched.schedList[e.ID]; exists {
					appSched.schedList[e.ID].NeedDbUpdate = true // todo mutex a utiliser
				}
			} else if e.dType == "DbTaskFlow" || e.dType == "DbQueue" {
				for i := range appSched.schedList {
					if (e.dType == "DbTaskFlow" && appSched.schedList[i].TaskFlowID == e.ID) || (e.dType == "DbQueue" && appSched.schedList[i].QueueID == e.ID) {
						appSched.schedList[e.ID].NeedDbUpdate = true // todo mutex a utiliser
					}
				}

			}
		}
		*/
		/* 		case periodid := <-appSched.instantStartTf:
		//flag de la tache comme à démarrer (action realisé par proceedSched quand taches en question dispo)
		if appSched.schedListInit {
			if _, exists := appSched.schedList[periodid]; exists {
				appSched.schedList[periodid].InstantStart = true // todo mutex a utiliser
			}
		} */
		case <-appSched.stopRequestCh:
			//arret du scheduleur
			fmt.Println("Sched stop request")
			//debloque le stop en attente
			close(appSched.terminatedCh)
			return

		case <-time.After(time.Second):
			//parcours des taches en cours
			appSched.taskMutex.Lock()
			if !appSched.schedListInit {
				//tant que liste des taches pa init
				updateSchedFromDb(0) //TODO log err ?
			} else {
				proceedSched()
			}
			appSched.taskMutex.Unlock()
		}
	}
}

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
	Stop()
	//lancement traitement
	fmt.Println("Sched starting")
	go pumpSched()
}

//UpdateSchedFromDb programme les eventuelles maj de tache a effectuer
func UpdateSchedFromDb(typeName string, ID int) {
	if appSched.checkDbCh != nil {
		appSched.checkDbCh <- chNotifyChange{
			dType: typeName,
			ID:    ID,
		}
	}
}

//InstantStart demande d'un démarrage immédiat
/* func InstantStart(periodid int) {
	if appSched.instantStartTf != nil {
		appSched.instantStartTf <- periodid
	}
} */

//updateSchedFromDb met à jour une tache depuis la bdd
func updateSchedFromDb(periodid int) error {
	//recup état de la planning à mettre à jour (tout si periodid==0)
	q := dal.SearchQuery{
		Limit:  2000,
		Offset: 0,
	}
	if periodid > 0 {
		q.SQLFilter = "id = ?"
		q.SQLParams = []interface{}{periodid}
	} else {
		q.SQLFilter = "(activ = 1 and deleted_at is null)"
	}

	/*arr, _, err := dal.SchedList(q)
	if err != nil {
		return fmt.Errorf("Sched updateSchedFromDb : " + err.Error())
	}*/

	updated := make(map[int]bool)
	/*for _, e := range arr {
			//on ne garde que les taches actives
			updated[e.ID] = true
			if !e.Activ || e.Deleted {
				delete(appSched.schedList, e.ID)
			} else {
				//recup sched, queue et taskflow
				var (
					q    dal.DbQueue
					tf   dal.DbTaskFlow
					vErr error
				)
				vErr = e.Validate(false) //tache invalide mis en mémoire mais non traitable
				if vErr == nil && e.QueueID > 0 {
					q, vErr = dal.QueueGet(e.QueueID)
					if vErr == nil && q.Deleted {
						vErr = fmt.Errorf("Queue %v [%v] disabled", q.ID, q.Lib)
					}
					if vErr == nil {
						vErr = q.Validate(false)
					}
				}
				if vErr == nil {
					tf, vErr = dal.TaskFlowGet(e.TaskFlowID)
					if vErr == nil && tf.Deleted {
						vErr = fmt.Errorf("TaskFlow %v [%v] disabled", tf.ID, tf.Lib)
					}
					if vErr == nil {
						vErr = tf.Validate(false)
					}
				}

				//maj info en mémoire
				if appSched.schedList[e.ID] == nil {
					appSched.schedList[e.ID] = &DbSchedState{}
				}
				appSched.schedList[e.ID].Activ = (vErr == nil)
				appSched.schedList[e.ID].ValidError = vErr
				appSched.schedList[e.ID].TaskFlowID = tf.ID
				appSched.schedList[e.ID].TaskFlow = tf
				appSched.schedList[e.ID].QueueID = q.ID
				appSched.schedList[e.ID].Queue = q
				appSched.schedList[e.ID].ErrLevel = e.ErrLevel
				appSched.schedList[e.ID].ID = e.ID
				appSched.schedList[e.ID].LastStart = e.LastStart
				appSched.schedList[e.ID].LastStop = e.LastStop
				appSched.schedList[e.ID].LastResult = e.LastResult
				appSched.schedList[e.ID].LastMsg = e.LastMsg
				appSched.schedList[e.ID].NeedDbUpdate = false
				appSched.schedList[e.ID].Detail = make([]dal.DbSchedDetail, len(e.Detail))
				copy(appSched.schedList[e.ID].Detail, e.Detail)
			}
	}*/

	//si maj full, on vire les flaggués
	if periodid == 0 {
		for k := range appSched.schedList {
			if updated[k] {
				delete(appSched.schedList, k)
			}
		}
		appSched.schedListInit = true
	}

	return nil
}

//proceedSched traite la liste des taches à executer
func proceedSched() {
	dtRef := time.Now()

	for _, v := range appSched.schedList {
		if v.ValidError == nil && !v.Running && !v.Queued { //tache non valide non traité
			if v.NeedDbUpdate {
				//maj info depuis db
				if err := updateSchedFromDb(v.ID); err != nil {
					fmt.Println("Sched", v.ID, "Err DB", err)
				}
				v.NeedDbUpdate = false
			} else if v.NextExec.IsZero() {
				//calcul date à exec
				v.NextExec = v.CalcNextLaunch(dtRef)
			} else if v.NextExec.Before(dtRef) {
				//date dépassé, on lance la tache
				go startSchedTask(v.ID)
			}
		}
	}

}

//startSchedTask lance une tache
func startSchedTask(periodid int) {
	s := appSched.schedList[periodid]
	s.Running = true
	defer func() {
		s.Running = false
	}()

	//démarrage immédiat
	/*
		if s.QueueID <= 0 {
			fmt.Println("Starting", periodid, s.NextExec, "taskflow", s.TaskFlow.Lib)
			s.NextExec = time.Time{} //raz pour recalcul
			time.Sleep(2 * time.Second)

		} else {
			// ajout a fil d'attente
			fmt.Println("Queueing", periodid, s.NextExec, "taskflow", s.TaskFlow.Lib)
			s.NextExec = time.Time{} //raz pour recalcul
			// TODO
			time.Sleep(2 * time.Second)
		}
	*/
	fmt.Println("Terminated", periodid, "taskflow", s.TaskFlow.Lib)
}
