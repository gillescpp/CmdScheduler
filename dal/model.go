package dal

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	hformat = "15:04:05"
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
	for i := range c.Detail {
		e := c.Detail[i].Validate(Create, len(c.Detail))
		if e != nil {
			return fmt.Errorf("detail %v : %v", (i + 1), e)
		}
		if c.Detail[i].Idx != (i + 1) {
			return fmt.Errorf("invalid idx order")
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
	if c.NextTaskIDOK < -1 || c.NextTaskIDOK > DetailListSize {
		return fmt.Errorf("invalid next task idx")
	}
	if c.NextTaskIDFail < -1 || c.NextTaskIDFail > DetailListSize {
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
// - LastStart / LastStop : plage dernier fonctionnement (UTC)
// - LastResult : cf. SchedRes
// - LastMsg : msg dernier résultat
// - Detail : liste des planif d'activation
type DbSched struct {
	ID         int       `json:"id"`
	TaskFlowID int       `json:"taskflowid" apiuse:"search,sort" dbfield:"SCHED.taskflowid"`
	ErrLevel   int       `json:"err_level" apiuse:"search,sort" dbfield:"SCHED.err_level"`
	QueueID    int       `json:"queueid" apiuse:"search,sort" dbfield:"SCHED.queueid"`
	Activ      bool      `json:"activ" apiuse:"search,sort" dbfield:"SCHED.activ"`
	LastStart  time.Time `json:"last_start" apiuse:"search,sort" dbfield:"SCHED.last_start"`
	LastStop   time.Time `json:"last_stop" apiuse:"search,sort" dbfield:"SCHED.last_stop"`
	LastResult int       `json:"last_result" apiuse:"search,sort" dbfield:"SCHED.last_result"`
	LastMsg    string    `json:"last_msg" apiuse:"search,sort" dbfield:"SCHED.last_msg"`
	TimeZone   string    `json:"time_zone" apiuse:"search,sort" dbfield:"SCHED.time_zone"`
	zone       *time.Location

	Detail []DbSchedDetail `json:"detail"`

	Deleted bool `json:"deleted" apiuse:"search,sort" dbfield:"SCHED.deleted_at"` /// todo pour filtrage des non actif ?
}

// DbSchedDetail détail activation sched
// - Interval : 0 si prog horaire fixe, ou tps en seconde pour type intervalle
// - IntervalHours : plages horaires 08:00:05-10:00:00,14:00:00-18:00:00 appliqué pour un type interval
// - Hours : liste horaire d'exec 08:00:05, 10:00:00 (shed type heure fixe)
// - Months : mois d'exex format JFMAMJJASOND : "01000100000" ou "*" pour tous
// - WeekDays : jours d'exex format LMMJVSD : "1111100" ou "*" pour tous
// - MonthDays : jours du mois sous forme de n° : "1,15", et ou code "1MON, 2TUE, FIRST, LAST"
//               (1er lundi du mois, 2eme mardi du mois, 1e j du mois, dernier j du mois) ou "*" pour tous
// Toutes les dates heures sont dans la tz fourni à la création
type DbSchedDetail struct {
	Interval      int    `json:"interval,omitempty" dbfield:"TASKFLOWDETAIL.interval"`
	IntervalHours string `json:"intervalhours,omitempty" dbfield:"TASKFLOWDETAIL.intervalhours"`
	Hours         string `json:"hours,omitempty" dbfield:"TASKFLOWDETAIL.hours"`
	Months        string `json:"months" dbfield:"TASKFLOWDETAIL.months"`
	WeekDays      string `json:"weekdays" dbfield:"TASKFLOWDETAIL.weekdays"`
	MonthDays     string `json:"monthdays" dbfield:"TASKFLOWDETAIL.monthdays"`

	zone *time.Location //rappel tz paren
	//intervalHours valeurs deserialisé : pair from->to
	intervalHoursFrom []time.Time
	intervalHoursTo   []time.Time
	//deserial Hours Months weekday
	hours    []time.Time // liste des heures (reprise des heures de la planif type heure fixe, ou calculé selon l'intervalle et les plage dispo)
	months   [12]bool
	weekDays [7]bool
	//deserial MonthDays
	monthDaysFilter   bool
	monthDaysDays     [31]bool
	monthDaysDaysSet  bool
	monthDaysKeywords map[string]bool //1MON=true FIRST=true, etc.
	monthDaysFirst    bool
	monthDaysLast     bool
}

// Validate pour controle de validité
func (c *DbSched) Validate(Create bool) error {
	if Create && c.ID > 0 {
		return fmt.Errorf("invalid create")
	} else if !Create && c.ID <= 0 {
		return fmt.Errorf("invalid id")
	}

	//qualif tz
	if c.TimeZone != "" {
		c.zone, _ = time.LoadLocation(c.TimeZone)
	}
	if c.zone == nil {
		c.zone = time.Local
	}
	c.TimeZone = c.zone.String()

	if !c.Deleted {
		task, _ := TaskGet(c.TaskFlowID)
		if task.ID == 0 {
			return fmt.Errorf("invalid taskflow id")
		}

		if c.ErrLevel < 0 {
			c.ErrLevel = 0
		}

		if c.QueueID != 0 {
			queue, _ := QueueGet(c.QueueID)
			if queue.ID == 0 {
				return fmt.Errorf("invalid queue id")
			}
		}

		// check détail
		if len(c.Detail) == 0 {
			return fmt.Errorf("invalid scheduling")
		}
		for i := range c.Detail {
			e := c.Detail[i].Validate(Create, c.zone)
			if e != nil {
				return fmt.Errorf("invalid scheduling %v : %v", (i + 1), e)
			}
		}
	}

	return nil
}

// CalcNextLaunch calcul prochaine heure d'exe > à dtRef
func (c *DbSched) CalcNextLaunch(dtRef time.Time) time.Time {
	if dtRef.IsZero() {
		return time.Time{}
	}

	dtRet := time.Time{}
	for i := range c.Detail {
		dtDet := c.Detail[i].CalcNextLaunch(dtRef)
		if !dtDet.IsZero() && (dtRet.IsZero() || dtDet.Before(dtRet)) {
			dtRet = dtDet
		}
	}
	return dtRet
}

// Validate pour controle de validité
func (c *DbSchedDetail) Validate(Create bool, zone *time.Location) error {
	c.zone = zone
	if c.Interval > 0 {
		c.Hours = ""
		//type interval
		errv1 := c.ValidateIntervalHours()
		errv2 := c.CalcDayHours()
		if errv1 != nil {
			return fmt.Errorf("invalid IntervalHours : %v", errv1)
		}
		if errv2 != nil {
			return fmt.Errorf("err CalcDayHours : %v", errv2)
		}
	} else {
		c.Interval = 0
		//heure fixe
		if c.Hours == "" {
			return fmt.Errorf("invalid hours")
		}
		//validation/normalisation
		errv := c.ValidateHours()
		if errv != nil {
			return fmt.Errorf("invalid Hours : %v", errv)
		}
	}
	//commun au deux type
	errv1 := c.ValidateWeekDays()
	errv2 := c.ValidateMonthDays()
	errv3 := c.ValidateMonths()
	if errv1 != nil {
		return fmt.Errorf("invalid WeekDays : %v", errv1)
	}
	if errv2 != nil {
		return fmt.Errorf("invalid MonthDays : %v", errv2)
	}
	if errv3 != nil {
		return fmt.Errorf("invalid Months : %v", errv3)
	}

	return nil
}

// ValidateHours validation deserialise
func (c *DbSchedDetail) ValidateHours() error {
	c.Hours = strings.ReplaceAll(c.Hours, " ", "")
	var errs = make([]string, 0)

	lst := strings.Split(c.Hours, ",")
	elms := make(map[time.Time]bool) //pour dédoublage
	for _, e := range lst {
		t, ep := time.ParseInLocation(hformat, e, c.zone)

		if ep != nil {
			errs = append(errs, e+" : parse fail")
		} else {
			elms[t] = true
		}
	}

	//puis extract
	c.hours = make([]time.Time, 0)
	for tv := range elms {
		c.hours = append(c.hours, tv)
	}
	//trie chrono
	sort.Slice(c.hours, func(i, j int) bool {
		return c.hours[i].Before(c.hours[j])
	})
	// recomposition
	c.Hours = ""
	for i := range c.hours {
		if c.Hours != "" {
			c.Hours += ","
		}
		c.Hours += c.hours[i].Format(hformat)
	}

	if len(elms) == 0 && len(c.hours) == 0 {
		errs = append(errs, "Hours empty")
	}

	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, ","))
	}
	return nil
}

// ValidateMonths validation deserialise
func (c *DbSchedDetail) ValidateMonths() error {
	c.Months = strings.ReplaceAll(c.Months, " ", "")
	if len(c.Months) != 12 {
		c.Months = "*"
		for i := 0; i < 12; i++ {
			c.months[i] = true
		}
	} else {
		clean := ""
		for i := 0; i < 12; i++ {
			c.months[i] = (c.Months[i] == '1')
			if c.months[i] {
				clean += "1"
			} else {
				clean += "0"
			}
		}
		c.Months = clean
	}
	return nil
}

// ValidateWeekDays validation deserialise
func (c *DbSchedDetail) ValidateWeekDays() error {
	c.WeekDays = strings.ReplaceAll(c.WeekDays, " ", "")
	if len(c.WeekDays) != 7 {
		c.WeekDays = "*"
		for i := 0; i < 7; i++ {
			c.weekDays[i] = true
		}
	} else {
		clean := ""
		for i := 0; i < 7; i++ {
			c.weekDays[i] = (c.WeekDays[i] == '1')
			if c.weekDays[i] {
				clean += "1"
			} else {
				clean += "0"
			}
		}
		c.WeekDays = clean
	}
	return nil
}

// ValidateMonthDays validation deserialise
func (c *DbSchedDetail) ValidateMonthDays() error {
	c.MonthDays = strings.ReplaceAll(c.MonthDays, " ", "")
	c.MonthDays = strings.ToUpper(c.MonthDays)
	if c.MonthDays == "" {
		c.MonthDays = "*"
	}
	c.monthDaysDaysSet = false
	c.monthDaysKeywords = make(map[string]bool)
	c.monthDaysFirst = false
	c.monthDaysLast = false
	c.monthDaysFilter = !(c.MonthDays == "*")

	if c.monthDaysFilter {
		for i := 0; i < 31; i++ {
			c.monthDaysDays[i] = false
		}

		//desialise des elements
		bFilter := false                        //au moins 1 filtre valide rencontré
		elms := strings.Split(c.MonthDays, ",") //1,31,1MON....

		for _, e := range elms {
			// FIRST, LAST
			if e == "FIRST" {
				c.monthDaysFirst = true
				bFilter = true
			} else if e == "LAST" {
				c.monthDaysLast = true
				bFilter = true
			} else if len(e) == 4 {
				//format (1-4)MON, 1TUE, 1WED, 1THU, 1FRI, 1SAT, 1SUN
				n, _ := strconv.Atoi(string(e[0])) //n°
				d := e[1:4]
				if n > 0 && n < 6 && (d == "MON" || d == "TUE" || d == "WED" ||
					d == "THU" || d == "FRI" || d == "SAT" || d == "SUN") {
					if _, exist := c.monthDaysKeywords[e]; !exist {
						c.monthDaysKeywords[e] = true
						bFilter = true
					}
				}
			} else {
				nd, _ := strconv.Atoi(e) //n° de jour
				if nd > 0 && nd < 32 {
					c.monthDaysDays[nd-1] = true
					c.monthDaysDaysSet = true
					bFilter = true
				}
			}
		}
		c.monthDaysFilter = bFilter
	}

	//rebuild v texte
	clean := ""
	if !c.monthDaysFilter {
		clean = "*"
	} else {
		if c.monthDaysFirst {
			clean += "FIRST,"
		}
		if c.monthDaysLast {
			clean += "LAST,"
		}
		for v := range c.monthDaysKeywords {
			clean += (v + ",")
		}
		for i := 0; c.monthDaysDaysSet && i < 31; i++ {
			if c.monthDaysDays[i] {
				clean += strconv.Itoa(i+1) + ","
			}
		}
		if len(clean) > 1 {
			clean = clean[0 : len(clean)-1]
		}
	}
	c.MonthDays = clean

	return nil
}

// ValidateIntervalHours valide et deserialise IntervalHours (08:00:05-10:00:00,14:00:00-18:00:00)
func (c *DbSchedDetail) ValidateIntervalHours() error {
	var errs = make([]string, 0)
	c.intervalHoursFrom = make([]time.Time, 0)
	c.intervalHoursTo = make([]time.Time, 0)
	c.IntervalHours = strings.ReplaceAll(c.IntervalHours, " ", "")
	if c.IntervalHours == "*" {
		c.IntervalHours = ""
	}

	lst := strings.Split(c.IntervalHours, ",")
	for _, e := range lst {
		if e == "" {
			continue
		}

		// 08:00:05-10:00:00
		lst := strings.Split(e, "-")
		if len(lst) != 2 {
			errs = append(errs, e+" : invalid")
		} else {
			t1, e1 := time.ParseInLocation(hformat, lst[0], c.zone)
			t2, e2 := time.ParseInLocation(hformat, lst[1], c.zone)
			if e1 != nil || e2 != nil {
				errs = append(errs, e+" : parse fail")
			} else if t2.Sub(t1).Seconds() > 0 { // la plage doit être > à 1s
				//plage valide, on la fait eventuellement fusioner avec l'existant en cas d'overflow
				//ex  08:00:00-10:00:00, 09:00:00-10:30:00 = 08:00:00-10:30:00
				fusionned := false
				for i := range c.intervalHoursFrom {
					if (t1.Before(c.intervalHoursFrom[i]) || t1.Equal(c.intervalHoursFrom[i])) &&
						(t2.After(c.intervalHoursTo[i]) || t2.Equal(c.intervalHoursTo[i])) {
						// nouvel interval englobant
						c.intervalHoursFrom[i] = t1
						c.intervalHoursTo[i] = t2
						fusionned = true
					} else if (t1.After(c.intervalHoursFrom[i]) || t1.Equal(c.intervalHoursFrom[i])) &&
						(t2.Before(c.intervalHoursTo[i]) || t2.Equal(c.intervalHoursTo[i])) {
						// nouvel interval englobé -> ignoré
						fusionned = true
					} else if t1.Before(c.intervalHoursTo[i]) || t1.Equal(c.intervalHoursTo[i]) {
						// nouvel interval alonge un existant (heure de fin)
						c.intervalHoursTo[i] = t2
						fusionned = true
					} else if t2.Before(c.intervalHoursFrom[i]) || t1.Equal(c.intervalHoursFrom[i]) {
						// nouvel interval alonge un existant (heure de debut)
						c.intervalHoursFrom[i] = t1
						fusionned = true
					}
					if fusionned {
						break
					}
				}
				if !fusionned {
					//new
					c.intervalHoursFrom = append(c.intervalHoursFrom, t1)
					c.intervalHoursTo = append(c.intervalHoursTo, t2)
				}
			}
		}
	}
	// Trie : la fusion fait que les intervalles overflow ne devrait pas pouvoir exister
	// donc les trie des paires indépendants ne doit pas fausser les intervelles
	sort.Slice(c.intervalHoursFrom, func(i, j int) bool {
		return c.intervalHoursFrom[i].Before(c.intervalHoursFrom[j])
	})
	sort.Slice(c.intervalHoursTo, func(i, j int) bool {
		return c.intervalHoursTo[i].Before(c.intervalHoursTo[j])
	})
	//reconstuction version serial
	c.IntervalHours = ""
	for i := range c.intervalHoursFrom {
		if c.IntervalHours != "" {
			c.IntervalHours += ","
		}
		c.IntervalHours += c.intervalHoursFrom[i].Format(hformat) + "-" + c.intervalHoursTo[i].Format(hformat)
	}

	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, ","))
	}
	return nil
}

// CalcDayHours calcules les heures de lancement sur la journée (planfi type intervalle)
func (c *DbSchedDetail) CalcDayHours() error {
	c.hours = make([]time.Time, 0)
	if c.Interval > 0 {
		//planning de type interval, on crée la liste des heures la journée
		if len(c.intervalHoursFrom) == 0 {
			dtCnt := time.Date(0, 1, 1, 0, 0, 0, 0, c.zone)
			dtEnd := time.Date(0, 1, 2, 0, 0, 0, 0, c.zone)
			for dtCnt.Before(dtEnd) {
				c.hours = append(c.hours, dtCnt)
				dtCnt = dtCnt.Add(time.Second * time.Duration(c.Interval))
			}
		} else {
			for h := range c.intervalHoursFrom {
				//1er heure de la plage intcluse, puis on incrémente jusqu'a la fin
				dtCnt := c.intervalHoursFrom[h]
				for dtCnt.Before(c.intervalHoursTo[h]) || dtCnt.Equal(c.intervalHoursTo[h]) {
					c.hours = append(c.hours, dtCnt)
					dtCnt = dtCnt.Add(time.Second * time.Duration(c.Interval))
				}
			}
		}
	}
	return nil
}

// CalcNextLaunch calcul prochaine heure d'exe > à dtRef
func (c *DbSchedDetail) CalcNextLaunch(dtRef time.Time) time.Time {
	if len(c.hours) == 0 || dtRef.IsZero() {
		return time.Time{}
	}
	//précision : seconde
	dtRef = dtRef.In(c.zone) //conv en tz du sched
	dtRef2 := time.Date(dtRef.Year(), dtRef.Month(), dtRef.Day(), dtRef.Hour(), dtRef.Minute(), dtRef.Second(), 0, c.zone)
	if dtRef2.Before(dtRef) {
		dtRef2 = dtRef2.Add(time.Second)
	}
	dtRef = dtRef2

	//recherche prochaine jour applicable
	dtDayTest := time.Date(dtRef.Year(), dtRef.Month(), dtRef.Day(), 0, 0, 0, 0, c.zone)
	for i := 0; i < 366; i++ { //1 an max
		if i > 0 { //nouvelle passe, on repart du lendemai 00:00
			dtDayTest = dtDayTest.AddDate(0, 0, 1)
		}
		//test mois applicable
		bMonthFound := false
		for m := 0; !bMonthFound; m++ {
			dtMonth := int(dtDayTest.Month())
			if c.months[dtMonth-1] {
				//mois applicable
				bMonthFound = true
			} else {
				//on pousse directement au mois suivant (overflow mois 13 géré par la lib std)
				dtDayTest = time.Date(dtDayTest.Year(), time.Month(dtMonth+1), 1, 0, 0, 0, 0, c.zone)
			}
		}
		if !bMonthFound {
			break
		}

		//qualif pour test jour applicable
		wDay := int(dtDayTest.Weekday()) - 1 //au format 0=lundi...
		if wDay == -1 {                      //Sunday=0-1=-1
			wDay = 6
		}
		mDay := dtDayTest.Day()                                                                                   // jour du mois
		firstMonthDay := (mDay == 1)                                                                              // 1er jour mois
		lastMonthDay := (dtDayTest.AddDate(0, 0, 1).Month() != dtDayTest.Month())                                 //dernier jour mois
		nj3 := strconv.Itoa(int((float64(dtDayTest.Day())-1.0)/7.0)+1) + strings.ToUpper(dtDayTest.Format("Mon")) //calcul code "1MON", "3TUE"...

		//jour de semaine ko
		if !c.weekDays[wDay] {
			continue
		}

		// test regle type <n><jour> ex:"1MON" (1 lundi du mois, 2eme mardi du mois...) monthDaysKeywords
		nj3kExists := false
		if c.monthDaysFilter && len(c.monthDaysKeywords) > 0 {
			_, nj3kExists = c.monthDaysKeywords[nj3]
		}

		// j du mois spécifiquement autorisé, ou 1er/dernier jour du mois, ou code
		monthDayAllowed := !c.monthDaysFilter ||
			(c.monthDaysDaysSet && c.monthDaysDays[mDay-1]) ||
			(c.monthDaysFirst && firstMonthDay) ||
			(c.monthDaysLast && lastMonthDay) ||
			nj3kExists
		if !monthDayAllowed {
			continue
		}

		// jour d'exec atteind, on charche l'heure applicable
		if dtDayTest.Day() == dtRef.Day() && dtDayTest.Month() == dtRef.Month() && dtDayTest.Year() == dtRef.Year() {
			dtFromHour := time.Date(0, 1, 1, dtRef.Hour(), dtRef.Minute(), dtRef.Second(), 0, c.zone)
			for h := range c.hours { // hours est trié chrono
				if c.hours[h].After(dtFromHour) {
					return time.Date(dtDayTest.Year(), dtDayTest.Month(), dtDayTest.Day(), c.hours[h].Hour(), c.hours[h].Minute(), c.hours[h].Second(), 0, c.zone)
				}
			}
			//si plus d'heure pour ce jour, il faut voir sur les jour suivants
		} else if len(c.hours) > 0 {
			//si le prochain jour est > à dtRef, alors on repart de minuit et donc de la premiere heure applicable
			return time.Date(dtDayTest.Year(), dtDayTest.Month(), dtDayTest.Day(), c.hours[0].Hour(), c.hours[0].Minute(), c.hours[0].Second(), 0, c.zone)
		}
	}

	return time.Time{}
}
