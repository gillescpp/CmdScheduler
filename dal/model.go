package dal

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// DbUser model utilisateur, table USER
type DbUser struct {
	ID         int    `json:"id"`
	Name       string `json:"name" apiuse:"search,sort" dbfield:"USER.name"`
	Login      string `json:"login" apiuse:"search,sort" dbfield:"USER.login"`
	RightLevel int    `json:"rightlevel"`
	Password   string `json:"password,omitempty"`
	Deleted    bool   `json:"deleted" apiuse:"search,sort" dbfield:"USER.deleted_at"` /// todo pour filtrage des non actif ?
}

// ValidatePassword pour controle de validité
func (c *DbUser) ValidatePassword() error {
	if c.Password != strings.TrimSpace(c.Password) {
		return fmt.Errorf("leading or trailing white spaces forbidden")
	}
	if len(c.Password) < 8 {
		return fmt.Errorf("password len is < 9")
	}
	num := `[0-9]{1}`
	az := `[a-z]{1}`
	AZ := `[A-Z]{1}`
	symbol := `[!@#~$%^&*()+|_]{1}`
	if b, err := regexp.MatchString(num, c.Password); !b || err != nil {
		return fmt.Errorf("password need num")
	}
	if b, err := regexp.MatchString(az, c.Password); !b || err != nil {
		return fmt.Errorf("password need a_z")
	}
	if b, err := regexp.MatchString(AZ, c.Password); !b || err != nil {
		return fmt.Errorf("password need A_Z")
	}
	if b, err := regexp.MatchString(symbol, c.Password); !b || err != nil {
		return fmt.Errorf("password need symbol")
	}
	return nil
}

// Validate pour controle de validité
func (c *DbUser) Validate(Create bool) error {
	if Create && c.ID > 0 {
		return fmt.Errorf("invalid create")
	} else if !Create && c.ID <= 0 {
		return fmt.Errorf("invalid id")
	}

	//create ou maj password
	if Create || c.Password != "" {
		err := c.ValidatePassword()
		if err != nil {
			return err
		}
	}

	// autre spec au mode create
	if Create {
		cleanLogin := strings.ToLower(c.Login)
		cleanLogin = strings.TrimSpace(c.Login)
		cleanLogin = strings.ReplaceAll(cleanLogin, " ", "-")
		if !strings.EqualFold(cleanLogin, c.Login) || len(cleanLogin) < 3 {
			return fmt.Errorf("invalid login")
		}
		c.Login = cleanLogin
		if !UserLoginAvailable(c.Login) {
			return fmt.Errorf("login not available")
		}
	}

	c.Name = strings.TrimSpace(c.Name)
	if strings.TrimSpace(c.Name) == "" {
		return fmt.Errorf("invalid name")
	}
	return nil
}

// DbAgent agent
type DbAgent struct {
	ID      int    `json:"id"`
	Host    string `json:"host" apiuse:"search,sort" dbfield:"AGENT.host"`
	APIKey  string `json:"apikey" apiuse:"search,sort" dbfield:"AGENT.apikey"`
	Deleted bool   `json:"deleted" apiuse:"search,sort" dbfield:"AGENT.deleted_at"` /// todo pour filtrage des non actif ?
}

// Validate pour controle de validité
func (c *DbAgent) Validate(Create bool) error {
	if Create && c.ID > 0 {
		return fmt.Errorf("invalid create")
	} else if !Create && c.ID <= 0 {
		return fmt.Errorf("invalid id")
	}

	// trim et check val
	c.Host = strings.TrimSpace(c.Host)
	if strings.TrimSpace(c.Host) == "" {
		return fmt.Errorf("invalid Host")
	}
	c.APIKey = strings.TrimSpace(c.APIKey)
	if strings.TrimSpace(c.APIKey) == "" {
		return fmt.Errorf("invalid APIKey")
	}

	return nil
}

// DbTask task
type DbTask struct {
	ID       int    `json:"id"`
	Lib      string `json:"lib" apiuse:"search,sort" dbfield:"TASK.lib"`
	Type     string `json:"type" apiuse:"search,sort" dbfield:"TASK.type"`
	Timeout  int    `json:"timeout" dbfield:"TASK.timeout"`
	LogStore string `json:"log_store" apiuse:"search,sort" dbfield:"TASK.log_store"`
	Cmd      string `json:"cmd" apiuse:"search,sort" dbfield:"TASK.cmd"`
	Args     string `json:"args" dbfield:"TASK.args"`
	StartIn  string `json:"start_in" dbfield:"TASK.start_in"`
	ExecOn   string `json:"exec_on" dbfield:"TASK.exec_on"`

	Deleted bool `json:"deleted" apiuse:"search,sort" dbfield:"TASK.deleted_at"` /// todo pour filtrage des non actif ?
}

// Validate pour controle de validité
func (c *DbTask) Validate(Create bool) error {
	if Create && c.ID > 0 {
		return fmt.Errorf("invalid create")
	} else if !Create && c.ID <= 0 {
		return fmt.Errorf("invalid id")
	}

	// type géré actuellement : CmdTask, URLCheckTask
	if c.Type != "CmdTask" && c.Type != "URLCheckTask" {
		return fmt.Errorf("invalid type")
	}

	c.Lib = strings.TrimSpace(c.Lib)
	if c.Lib == "" {
		return fmt.Errorf("invalid lib")
	}
	if c.Timeout < 0 {
		c.Timeout = 0
	}
	c.Cmd = strings.TrimSpace(c.Cmd)
	if c.Cmd == "" {
		return fmt.Errorf("invalid cmd")
	}
	c.Args = strings.TrimSpace(c.Args)
	c.StartIn = strings.TrimSpace(c.StartIn)
	c.ExecOn = strings.TrimSpace(c.ExecOn)

	c.LogStore = strings.TrimSpace(c.LogStore)
	c.LogStore = strings.ReplaceAll(c.LogStore, "  ", " ")
	c.LogStore = strings.ReplaceAll(c.LogStore, " ", "-")
	c.LogStore = strings.ToLower(c.LogStore)

	return nil
}

// DbQueue queue
type DbQueue struct {
	ID      int    `json:"id"`
	Lib     string `json:"lib" apiuse:"search,sort" dbfield:"QUEUE.lib"`
	Size    int    `json:"size" apiuse:"search,sort" dbfield:"QUEUE.size"`
	Timeout int    `json:"timeout" dbfield:"QUEUE.timeout"`

	Deleted bool `json:"deleted" apiuse:"search,sort" dbfield:"QUEUE.deleted_at"` /// todo pour filtrage des non actif ?
}

// Validate pour controle de validité
func (c *DbQueue) Validate(Create bool) error {
	if Create && c.ID > 0 {
		return fmt.Errorf("invalid create")
	} else if !Create && c.ID <= 0 {
		return fmt.Errorf("invalid id")
	}

	c.Lib = strings.TrimSpace(c.Lib)
	if strings.TrimSpace(c.Lib) == "" {
		return fmt.Errorf("invalid lib")
	}
	if c.Size < 0 {
		c.Size = 0
	}
	if c.Timeout < 0 {
		c.Timeout = 0
	}
	return nil
}

// DbTag tag
type DbTag struct {
	ID    int    `json:"id"`
	Lib   string `json:"lib" apiuse:"search,sort" dbfield:"TAG.lib"`
	Group string `json:"group" apiuse:"search,sort" dbfield:"TAG.tgroup"`

	Deleted bool `json:"deleted" apiuse:"search,sort" dbfield:"TAG.deleted_at"` /// todo pour filtrage des non actif ?
}

// Validate pour controle de validité
func (c *DbTag) Validate(Create bool) error {
	if Create && c.ID > 0 {
		return fmt.Errorf("invalid create")
	} else if !Create && c.ID <= 0 {
		return fmt.Errorf("invalid id")
	}

	c.Lib = strings.TrimSpace(c.Lib)
	c.Lib = strings.ReplaceAll(c.Lib, "  ", " ")
	c.Lib = strings.ReplaceAll(c.Lib, " ", "-")
	c.Lib = strings.ToLower(c.Lib)
	if c.Lib == "" {
		return fmt.Errorf("invalid lib")
	}

	c.Group = strings.TrimSpace(c.Group)
	c.Group = strings.ReplaceAll(c.Group, "  ", " ")
	c.Group = strings.ReplaceAll(c.Group, " ", "-")
	c.Group = strings.ToLower(c.Group)
	if c.Group == "" {
		return fmt.Errorf("invalid group")
	}

	return nil
}

// DbTaskFlow description tache à executer
type DbTaskFlow struct {
	ID     int                `json:"id"`
	Lib    string             `json:"lib" apiuse:"search,sort" dbfield:"TASKFLOW.lib"`
	Tags   string             `json:"tags" apiuse:"search,sort" dbfield:"TASKFLOW.tags"`
	Detail []DbTaskFlowDetail `json:"detail"`

	Deleted bool `json:"deleted" apiuse:"search,sort" dbfield:"TASKFLOW.deleted_at"` /// todo pour filtrage des non actif ?
}

// DbTaskFlowDetail détail task flow
type DbTaskFlowDetail struct {
	Idx            int `json:"idx"`
	TaskID         int `json:"taskid" dbfield:"TASKFLOWDETAIL.taskid"` //idx base 0
	NextTaskIDOK   int `json:"nexttaskid_ok" dbfield:"TASKFLOWDETAIL.nexttaskid_ok"`
	NextTaskIDFail int `json:"nexttaskid_fail" dbfield:"TASKFLOWDETAIL.nexttaskid_fail"`
	NotifFail      int `json:"notiffail" dbfield:"TASKFLOWDETAIL.notiffail"`
}

// Validate pour controle de validité
func (c *DbTaskFlow) Validate(Create bool) error {
	if Create && c.ID > 0 {
		return fmt.Errorf("invalid create")
	} else if !Create && c.ID <= 0 {
		return fmt.Errorf("invalid id")
	}

	// autre spec au mode create
	c.Lib = strings.TrimSpace(c.Lib)
	if strings.TrimSpace(c.Lib) == "" {
		return fmt.Errorf("invalid lib")
	}

	// check détail
	if len(c.Detail) == 0 {
		return fmt.Errorf("empty task not allowed")
	}
	for i, d := range c.Detail {
		e := d.Validate(Create, len(c.Detail))
		if e != nil {
			return fmt.Errorf("detail %v : %v", (i + 1), e)
		}
	}

	return nil
}

// Validate pour controle de validité
func (c *DbTaskFlowDetail) Validate(Create bool, DetailListSize int) error {
	task, _ := TaskGet(c.TaskID)
	if task.ID == 0 {
		return fmt.Errorf("invalid task id")
	}
	if c.NextTaskIDOK < 0 || c.NextTaskIDOK >= DetailListSize {
		return fmt.Errorf("invalid next task idx")
	}
	if c.NextTaskIDFail < 0 || c.NextTaskIDFail >= DetailListSize {
		return fmt.Errorf("invalid next onfail task idx")
	}
	return nil
}

const (
	// SchedResUN DbSched.LastResult non connu
	SchedResUN = 0
	// SchedResOK DbSched.LastResult rés ok
	SchedResOK = 1
	// SchedResKO DbSched.LastResult rés ko
	SchedResKO = -1
)

// DbSched représente une planif d'un tache :
// - TaskFlowID : Id du task flow à executer
// - ErrLevel : code gestion d'erreur
// - QueueID : si soumis à une queue donnée
// - Activ : flag activation
// - LastStart / LastStop : plage dernier fonctionnement
// - LastResult : cf. SchedRes
// - LastMsg : msg dernier résultat
// - Detail : liste des planif d'activation
type DbSched struct {
	ID         int       `json:"id"`
	TaskFlowID int       `json:"taskflowid" apiuse:"search,sort" dbfield:"SCHED.taskflowid"`
	ErrLevel   int       `json:"err_level" apiuse:"search,sort" dbfield:"SCHED.err_level"`
	QueueID    int       `json:"queueid" apiuse:"search,sort" dbfield:"SCHED.queueid"`
	Activ      int       `json:"activ" apiuse:"search,sort" dbfield:"SCHED.activ"`
	LastStart  time.Time `json:"last_start" apiuse:"search,sort" dbfield:"SCHED.last_start"`
	LastStop   time.Time `json:"last_stop" apiuse:"search,sort" dbfield:"SCHED.last_stop"`
	LastResult int       `json:"last_result" apiuse:"search,sort" dbfield:"SCHED.last_result"`
	LastMsg    string    `json:"last_msg" apiuse:"search,sort" dbfield:"SCHED.last_msg"`

	Detail []DbSchedDetail `json:"detail"`

	Deleted bool `json:"deleted" apiuse:"search,sort" dbfield:"SCHED.deleted_at"` /// todo pour filtrage des non actif ?
}

// DbSchedDetail détail activation sched
// - Interval : 0 si prog horaire fixe, 1 si type intervalle
// - FixedInterval : 1 (horaire de démarage prédictible), 0 (interval depuis dernier stop (ou start a defaut), 2 (délai entre start)
// - IntervalHours : plages horaires 08:00:05-10:00:00,14:00:00-18:00:00 appliqu" pour un type interval
// - Hours : liste horaire d'exec 08:00:05, 10:00:00 (shed type heure fixe)
// - Months : mois d'exex format JFMAMJJASOND : "01000100000" ou "*" pour tous
// - WeekDays : jours d'exex format LMMJVSD : "1111100" ou "*" pour tous
// - MonthDays : jours du mois sous forme de n° : "1,15", et ou code "1MON, 2TUE, FIRST, LAST"
//               (1er lundi du mois, 2eme mardi du mois, 1e j du mois, dernier j du mois) ou "*" pour tous
type DbSchedDetail struct {
	Idx           int    `json:"idx"`
	Interval      int    `json:"interval" dbfield:"TASKFLOWDETAIL.interval"`
	FixedInterval int    `json:"fixedinterval" dbfield:"TASKFLOWDETAIL.fixedinterval"`
	IntervalHours string `json:"intervalhours" dbfield:"TASKFLOWDETAIL.intervalhours"`
	Hours         string `json:"hours" dbfield:"TASKFLOWDETAIL.hours"`
	Months        string `json:"months" dbfield:"TASKFLOWDETAIL.months"`
	WeekDays      string `json:"weekdays" dbfield:"TASKFLOWDETAIL.weekdays"`
	MonthDays     string `json:"monthdays" dbfield:"TASKFLOWDETAIL.monthdays"`
}

// Validate pour controle de validité
func (c *DbSched) Validate(Create bool) error {
	if Create && c.ID > 0 {
		return fmt.Errorf("invalid create")
	} else if !Create && c.ID <= 0 {
		return fmt.Errorf("invalid id")
	}

	if c.Activ == 1 && !c.Deleted {
		task, _ := TaskGet(c.TaskFlowID)
		if task.ID == 0 {
			return fmt.Errorf("invalid taskflow id")
		}

		queue, _ := QueueGet(c.QueueID)
		if queue.ID == 0 {
			return fmt.Errorf("invalid queue id")
		}

		// check détail
		if len(c.Detail) == 0 {
			return fmt.Errorf("invalid scheduling")
		}
		for i, d := range c.Detail {
			e := d.Validate(Create)
			if e != nil {
				return fmt.Errorf("invalid scheduling %v : %v", (i + 1), e)
			}
		}
	}

	return nil
}

// Validate pour controle de validité
func (c *DbSchedDetail) Validate(Create bool) error {
	if c.Interval == 1 {
		//type interval
		if c.FixedInterval < 0 || c.FixedInterval > 2 {
			return fmt.Errorf("invalid fixedinterval")
		}
		if c.Interval < 1 {
			return fmt.Errorf("invalid interval")
		}
		if c.IntervalHours != "" {
			//TODO : validation IntervalHours : plages horaires 08:00:05-10:00:00,14:00:00-18:00:00 appliqu" pour un type interval
		}
	} else {
		//heure fixe
		if c.Hours == "" {
			return fmt.Errorf("invalid hours")
		}
		//c.Hours	//TODO : validation hours : liste horaire d'exec 08:00:05, 10:00:00
		//c.Months	//TODO : validation months : mois d'exex format JFMAMJJASOND : "01000100000" ou "*" pour tous
		//c.WeekDays	//TODO : validation weekdays : jours d'exex format LMMJVSD : "1111100" ou "*" pour tous
		//c.MonthDays	//TODO : validation monthdays :
		//                       jours du mois sous forme de n° : "1,15", et ou code "1MON, 2TUE, FIRST, LAST"
		//                       (1er lundi du mois, 2eme mardi du mois, 1e j du mois, dernier j du mois) ou "*" pour tous
	}
	return nil
}
