package dal

// const pour le test
const (
	testUsr = 9
)

/*
// TestRes resultat attendu
type testRes struct {
	dataIn        interface{}
	dataOut       interface{}
	WaitedResOK   bool
	WaitedResInfo string
}

// TestAgent
func TestAgent(t *testing.T) {
	// jeu de test
	arr := []testRes{
		{
			dataIn: &DbAgent{
				ID:     0,
				Host:   "localhost:8080",
				APIKey: "APIKEY",
			},
			dataOut: &DbAgent{
				ID:     1,
				Host:   "localhost:8080",
				APIKey: "APIKEY",
			},
			WaitedResOK:   true,
			WaitedResInfo: "",
		},
		{
			dataIn: &DbAgent{
				ID:     0,
				Host:   "localhost:8080",
				APIKey: "APIKEY",
			},
			dataOut: &DbAgent{
				ID:     0,
				Host:   "",
				APIKey: "",
			},
			WaitedResOK:   false,
			WaitedResInfo: "Dupliqué",
		},
		{
			dataIn: &DbAgent{
				ID:     0,
				Host:   "localhost:8090",
				APIKey: "APIKEY2",
			},
			dataOut: &DbAgent{
				ID:     2,
				Host:   "localhost:8090",
				APIKey: "APIKEY2",
			},
			WaitedResOK:   true,
			WaitedResInfo: "",
		},
	}

	//validate
	agtVOK := &DbAgent{
		ID:     2,
		Host:   "localhost:8090",
		APIKey: "APIKEY2",
	}
	if agtVOK.Validate(false) != nil {
		t.Errorf("resultat agtVOK/validate ko")
	}
	agtVKO := &DbAgent{
		ID:     2,
		Host:   "localhost:8090",
		APIKey: "",
	}
	if agtVKO.Validate(false) == nil {
		t.Errorf("resultat agtVOK/validate ko")
	}

	//inserts
	for i, r := range arr {
		agt := r.dataIn.(*DbAgent)
		err := AgentInsert(agt, testUsr)
		resB := (err == nil)
		if resB != r.WaitedResOK {
			t.Errorf("resultat insert ok/ko inattendu l %v (%v)", i, err)
		}
	}

	//get
	for _, r := range arr {
		agt := r.dataOut.(*DbAgent)
		agtg, err := AgentGet(agt.ID)
		resB := (agtg.ID == agt.ID)
		if resB != true {
			t.Errorf("resultat get attendu <>, err %v", err)
		}
	}

	//AgentHostNotExists
	if AgentHostNotExists("localhost:8090") {
		t.Errorf("resultat AgentHostNotExists attendue true")
	}
	if !AgentHostNotExists("localhost:8999") {
		t.Errorf("resultat AgentHostNotExists attendue false")
	}
}

// TestQueue
func TestQueue(t *testing.T) {
	// jeu de test
	arr := []testRes{
		{
			dataIn: &DbQueue{
				ID:      0,
				Lib:     "Q1",
				Size:    300,
				Timeout: 10,
			},
			dataOut: &DbQueue{
				ID:      1,
				Lib:     "Q1",
				Size:    300,
				Timeout: 10,
			},
			WaitedResOK:   true,
			WaitedResInfo: "",
		},
		{
			dataIn: &DbQueue{
				ID:      0,
				Lib:     "Q1",
				Size:    300,
				Timeout: 10,
			},
			dataOut: &DbQueue{
				ID:      0,
				Lib:     "",
				Size:    0,
				Timeout: 0,
			},
			WaitedResOK:   false,
			WaitedResInfo: "doublon",
		},
	}

	//validate
	agtVOK := &DbQueue{
		ID:   50,
		Lib:  "TA1",
		Size: -300,
	}
	if agtVOK.Validate(false) != nil {
		t.Errorf("resultat agtVOK/validate ko")
	}
	agtVKO := &DbQueue{
		ID:  51,
		Lib: "",
	}
	if agtVKO.Validate(false) == nil {
		t.Errorf("resultat agtVOK/validate ko")
	}

	//inserts
	for i, r := range arr {
		q := r.dataIn.(*DbQueue)
		err := QueueInsert(q, testUsr)
		resB := (err == nil)
		if resB != r.WaitedResOK {
			t.Errorf("resultat insert ok/ko inattendu l %v (%v)", i, err)
		}
	}

	//get
	for _, r := range arr {
		q := r.dataOut.(*DbQueue)
		qt, err := QueueGet(q.ID)
		resB := (q.ID == qt.ID)
		if resB != true {
			t.Errorf("resultat get attendu <>, err %v", err)
		}
	}
}

// TestTask
func TestTask(t *testing.T) {
	// jeu de test
	arr := []testRes{
		{
			dataIn: &DbTag{
				ID:    0,
				Lib:   "Tag1",
				Group: "Main",
			},
			dataOut: &DbTag{
				ID:    1,
				Lib:   "Tag1",
				Group: "Main",
			},
			WaitedResOK:   true,
			WaitedResInfo: "",
		},
		{
			dataIn: &DbTag{
				ID:    0,
				Lib:   "Tag1",
				Group: "Main",
			},
			dataOut: &DbTag{
				ID:    0,
				Lib:   "",
				Group: "",
			},
			WaitedResOK:   false,
			WaitedResInfo: "doublon",
		},
	}

	//validate
	tVOK := &DbTag{
		ID:    50,
		Lib:   "TA1",
		Group: "321",
	}
	if tVOK.Validate(false) != nil {
		t.Errorf("resultat validate ko")
	}
	tVKO := &DbTag{
		Lib:   "TA1",
		Group: "",
	}
	if tVKO.Validate(false) == nil {
		t.Errorf("resultat validate ko")
	}

	//inserts
	for i, r := range arr {
		q := r.dataIn.(*DbTag)
		err := TagInsert(q, testUsr)
		resB := (err == nil)
		if resB != r.WaitedResOK {
			t.Errorf("resultat insert ok/ko inattendu l %v (%v)", i, err)
		}
	}

	//get
	for _, r := range arr {
		q := r.dataOut.(*DbTag)
		qt, err := TagGet(q.ID)
		resB := (q.ID == qt.ID)
		if resB != true {
			t.Errorf("resultat get attendu <>, err %v", err)
		}
	}
}

// TestTag
func TestTag(t *testing.T) {
	// jeu de test
	arr := []testRes{
		{
			dataIn: &DbTask{
				ID:       0,
				Lib:      "Task 1",
				Type:     "CmdTask",
				Timeout:  600,
				LogStore: "mylog",
				Cmd:      "c:\\app.exe",
				Args:     "-d bla",
				ExecOn:   "1,2",
			},
			dataOut: &DbTask{
				ID:       1,
				Lib:      "Task 1",
				Type:     "CmdTask",
				Timeout:  600,
				LogStore: "mylog",
				Cmd:      "c:\\app.exe",
				Args:     "-d bla",
				ExecOn:   "1,2",
			},
			WaitedResOK:   true,
			WaitedResInfo: "",
		},
	}

	//validate
	tVOK := &DbTask{
		ID:       1,
		Lib:      "Task 1",
		Type:     "CmdTask",
		Timeout:  600,
		LogStore: "mylog",
		Cmd:      "c:\\app.exe",
		Args:     "-d bla",
		ExecOn:   "1,2",
	}
	if tVOK.Validate(false) != nil {
		t.Errorf("resultat validate ko")
	}
	tVKO := &DbTask{
		ID:       1,
		Lib:      "Task 1",
		Type:     "CmdTaskU",
		Timeout:  600,
		LogStore: "mylog",
		Cmd:      "c:\\app.exe",
		Args:     "-d bla",
		ExecOn:   "1,2",
	}
	if tVKO.Validate(false) == nil {
		t.Errorf("resultat validate ko")
	}

	//inserts
	for i, r := range arr {
		q := r.dataIn.(*DbTask)
		err := TaskInsert(q, testUsr)
		resB := (err == nil)
		if resB != r.WaitedResOK {
			t.Errorf("resultat insert ok/ko inattendu l %v (%v)", i, err)
		}
	}

	//get
	for _, r := range arr {
		q := r.dataOut.(*DbTask)
		qt, err := TaskGet(q.ID)
		resB := (q.ID == qt.ID)
		if resB != true {
			t.Errorf("resultat get attendu <>, err %v", err)
		}
	}
}

// TestUser
func TestUser(t *testing.T) {
	// jeu de test
	arr := []testRes{
		{
			dataIn: &DbUser{
				ID:         0,
				Name:       "John 1",
				Login:      "j123",
				RightLevel: 50,
				Password:   "mypassMop12#",
			},
			dataOut: &DbUser{
				ID:         1,
				Name:       "John 1",
				Login:      "j123",
				RightLevel: 50,
				Password:   "",
			},
			WaitedResOK:   true,
			WaitedResInfo: "",
		},
		{
			dataIn: &DbUser{
				ID:         0,
				Name:       "John 2",
				Login:      "j123",
				RightLevel: 50,
				Password:   "mypass",
			},
			dataOut: &DbUser{
				ID:         0,
				Name:       "",
				Login:      "",
				RightLevel: 0,
				Password:   "",
			},
			WaitedResOK:   false,
			WaitedResInfo: "doublon",
		},
	}

	//validate
	tVOK := &DbUser{
		ID:         0,
		Name:       "John 1",
		Login:      "j123",
		RightLevel: 50,
		Password:   "mypassMop12#",
	}
	if tVOK.Validate(true) != nil {
		t.Errorf("resultat validate ko")
	}
	tVKO := &DbUser{
		ID:         1,
		Name:       "John 1",
		Login:      "j123",
		RightLevel: 50,
		Password:   "mypass",
	}
	if tVKO.Validate(false) == nil {
		t.Errorf("resultat validate ko")
	}

	//inserts
	for i, r := range arr {
		q := r.dataIn.(*DbUser)
		err := UserInsert(q, testUsr)
		resB := (err == nil)
		if resB != r.WaitedResOK {
			t.Errorf("resultat insert ok/ko inattendu l %v (%v)", i, err)
		}
	}

	//get
	for _, r := range arr {
		q := r.dataOut.(*DbUser)
		qo := r.dataIn.(*DbUser)
		qt, err := UserGet(q.ID)
		resB := (q.ID == qt.ID)
		if resB != true {
			t.Errorf("resultat get attendu <>, err %v", err)
		}
		if r.WaitedResOK {
			res, err := UserCheckAuth(q.Login, qo.Password)
			if err != nil || res == 0 {
				t.Errorf("resultat auth attendu <>, err %v", err)
			}
			res, err = UserCheckAuth(q.Login, "lmml")
			if res > 0 {
				t.Errorf("resultat auth attendu <>")
			}
		}
	}
}

// TestSched
func TestSched(t *testing.T) {
	// jeu de test
	tz := time.Local
	arr := []testRes{
		{
			dataIn: &DbSched{
				ID:         1,
				TaskFlowID: 1,
				ErrLevel:   1,
				QueueID:    0,
				Activ:      true,
				LastStart:  time.Time{},
				LastStop:   time.Time{},
				LastResult: 0,
				LastMsg:    "",
				zone:       tz,

				Detail: []DbSchedDetail{
					{
						Interval:      7200,
						IntervalHours: "08:00:00-09:00:00",
						MonthDays:     "1MON, 1TUE",
					},
					{
						Hours:     "11:00:00",
						MonthDays: "FIRST",
						Months:    "110000000000",
					},
				},
			},
			//dataOut: &DbTask{
		},
	}

	//test calcul
	dtRef := time.Now()
	for s, r := range arr {
		q := r.dataIn.(*DbSched)
		q.Validate(false)

		for d := range q.Detail {
			//detail
			ev := q.Detail[d].Validate(false, tz)
			if ev != nil {
				t.Errorf("Err validate %v, d %v : %v", s, d, ev)
				continue
			}

			dr := dtRef
			for i := 0; i < 15; i++ {
				dr = q.Detail[d].CalcNextLaunch(dr)
				if dr.IsZero() {
					fmt.Println(fmt.Sprintf("Sched %v, d %v INACTIF", s, d))
					break
				}
				fmt.Println(fmt.Sprintf("Sched %v, d %v, next lauch = %v", s, d, dr.Format(time.RFC850)))
				dr = dr.Add(time.Second)
			}
		}
	}
}

// TestMain main test
func TestMain(m *testing.M) {
	err := setup()
	if err != nil {
		fmt.Println("setup fail", err)
		os.Exit(1)
	}
	code := m.Run()
	shutdown()
	os.Exit(code)
}

// setup tearup
func setup() error {
	testfile := "test_data.db"
	os.Remove(testfile)
	_, err := os.Stat(testfile)
	if !os.IsNotExist(err) {
		return fmt.Errorf("bd test no supprimable")
	}
	err = InitDb("sqlite3", "file:"+testfile, "TESTSCHEME")
	if err == nil {
		//relance initDbTables pour chech que ça fonctionne aux dem suivants
		err = initDbTables()
	}
	return err
}

// shutdown teardown
func shutdown() {

}
*/
