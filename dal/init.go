package dal

import (
	"CmdScheduler/slog"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	//sqlite
	_ "github.com/mattn/go-sqlite3"
	//mssql
	_ "github.com/denisenkom/go-mssqldb"
)

//instance globale bdd
var (
	MainDB    *sql.DB
	tblPrefix string
	dbDriver  string
)

// InitDb prepa bdd mais
func InitDb(driver string, datasource string, prefix string) error {
	//conn/création
	dbPrefix := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(prefix), " ", "_"))
	dbDriver = driver
	tblPrefix = ""
	if dbPrefix != "" {
		tblPrefix = dbPrefix + "_"
	}

	var err error
	MainDB, err = sql.Open(dbDriver, datasource)
	if err != nil {
		return fmt.Errorf("open DB : %w", err)
	}
	if strings.EqualFold(dbDriver, "sqlite3") {
		MainDB.SetMaxOpenConns(1)
	}
	return initDbTables()
}

// updDbVersion init/set table version
func updDbVersion(dbversion *int) error {
	var err error
	//init table config kv
	if *dbversion <= 0 {
		if strings.EqualFold(dbDriver, "mssql") {
			initKv := `IF NOT EXISTS (select 1 from INFORMATION_SCHEMA.TABLES
			where TABLE_SCHEMA = ? AND TABLE_NAME = ?) 
			CREATE TABLE ` + tblPrefix + `CFG (KID varchar(100), KVAL varchar(1000), PRIMARY KEY(KID)) `
			_, err = MainDB.Exec(initKv, "dbo", tblPrefix+"CFG") //schema non géré
		} else {
			initKv := `CREATE TABLE IF NOT EXISTS ` + tblPrefix +
				`CFG (KID varchar(100), KVAL varchar(1000), PRIMARY KEY(KID)) `
			_, err = MainDB.Exec(initKv)
		}
		if err != nil {
			return err
		}

		//recup version en cours
		v, err := CfgKVGet("db.version")
		if err != nil {
			return err
		}
		*dbversion, _ = strconv.Atoi(v)

	} else {
		//maj version
		err = CfgKVSet("db.version", strconv.Itoa(*dbversion))
		if err != nil {
			return err
		}
	}
	return nil
}

// initDbTables modif DML avec maj vcersion db
func versionedDML(newVersion int, curVersion *int, sql string) error {
	if *curVersion < newVersion {
		slog.Trace("dal", "Update dc schema v %v...", newVersion)
		_, err := MainDB.Exec(sql)
		if err == nil {
			*curVersion = newVersion
			err = updDbVersion(curVersion)
		}
		if err != nil {
			return fmt.Errorf("initDbTables %v %w", curVersion, err)
		}
	}
	return nil
}

// initDbTables création / maj schema base
func initDbTables() error {
	curVersion := 0

	err := updDbVersion(&curVersion)
	if err != nil {
		return err
	}

	dttype := "datetime"
	autoinc := "INTEGER NOT NULL IDENTITY"
	if strings.EqualFold(dbDriver, "sqlite3") {
		autoinc = "INTEGER PRIMARY KEY"
	}
	if strings.EqualFold(dbDriver, "mssql") {
		dttype = "datetime2"
	}

	// note : protection des nom de colonne non portable `` pour mysql, [] pour mssql
	// donc pas d'espace dans les nom de champs et table
	// rajout de versionedDML a faire en dessous des autres

	// USER, rightlevel indique un niveau de droit (system de droit basique)
	iv := 1
	sql := `CREATE TABLE ` + tblPrefix + `USR (
		id ` + autoinc + `,
		name VARCHAR(150),      
		login VARCHAR(60),      
		rightlevel int,	        
		password VARCHAR(60),   
		deleted_at ` + dttype + `, deleted_by int,
		created_at ` + dttype + `, created_by int,
		updated_at ` + dttype + `, updated_by int
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//login unic pour les comptes actif
	sql = `CREATE UNIQUE INDEX IDX_CLI_UNIC_LOGIN ON ` + tblPrefix + `USR(login) where deleted_at is null`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE INDEX IDX_CLI_DELETED ON ` + tblPrefix + `USR(deleted_at)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	// 1ere init, on insere un user admin par defaut
	usr, _, _ := UserList(SearchQuery{
		Limit: 1,
	})
	if len(usr) == 0 {
		UserInsert(&DbUser{
			Name:       "Admin",
			Login:      "admin",
			RightLevel: RightLvlAdmin,
			Password:   "admin",
			Deleted:    false,
		}, 1)
	}

	// agents d'exec
	sql = `CREATE TABLE ` + tblPrefix + `AGENT (
		id ` + autoinc + `,
		host VARCHAR(150),             
		apikey VARCHAR(260),
		certsignallowed VARCHAR(512),
		deleted_at ` + dttype + `, deleted_by int,
		created_at ` + dttype + `, created_by int,
		updated_at ` + dttype + `, updated_by int
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE UNIQUE INDEX IDX_AGENT_UNIC_HOST ON ` + tblPrefix + `AGENT(host)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//type de tache calé sur les attendues agent : CmdTask, URLCheckTask
	sql = `CREATE TABLE ` + tblPrefix + `TASK (
			id ` + autoinc + `,
			lib VARCHAR(100),
			type VARCHAR(20),
			timeout INT,
			log_store VARCHAR(50),
			cmd VARCHAR(500),
			args VARCHAR(500),
			start_in VARCHAR(500),
			exec_on VARCHAR(250),
			created_at ` + dttype + `, created_by int,
			updated_at ` + dttype + `, updated_by int
			)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//tags
	sql = `CREATE TABLE ` + tblPrefix + `TAG (
		id ` + autoinc + `,
		lib VARCHAR(50),
		tgroup VARCHAR(50),
		created_at ` + dttype + `, created_by int,
		updated_at ` + dttype + `, updated_by int
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE UNIQUE INDEX IDX_TAG_LIB ON ` + tblPrefix + `TAG (lib, tgroup)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//queue
	sql = `CREATE TABLE ` + tblPrefix + `QUEUE (
		id ` + autoinc + `,
		lib VARCHAR(100),
		size int,
		timeout int,
		pausedfrom ` + dttype + `,
		noexecwhile_queuelist varchar(100),
		created_at ` + dttype + `, created_by int,
		updated_at ` + dttype + `, updated_by int
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE UNIQUE INDEX IDX_QUEUE_LIB ON ` + tblPrefix + `QUEUE (lib)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//periode, ou planif
	sql = `CREATE TABLE ` + tblPrefix + `PERIOD (
		id ` + autoinc + `,
		lib VARCHAR(100),
		type int,
		time_zone varchar(50),
		created_at ` + dttype + `, created_by int,
		updated_at ` + dttype + `, updated_by int
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//period détail
	sql = `CREATE TABLE ` + tblPrefix + `PERIODDETAIL (
		periodid int, idx int,
		interval int,
		intervalhours varchar(500),
		hours varchar(500),
		months varchar(12),
		weekdays varchar(7),
		monthdays varchar(100),
		primary key(periodid, idx)
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//task flow head
	sql = `CREATE TABLE ` + tblPrefix + `TASKFLOW (
		id ` + autoinc + `,
		lib VARCHAR(100),
		tags VARCHAR(100),
		activ int,
		named_args VARCHAR(500),
		manuallaunch int,
		scheduleid int,
		err_management int,
		queueid int,
		last_start ` + dttype + `,
		last_stop ` + dttype + `,
		last_result int,
		last_msg varchar(4000),
		created_at ` + dttype + `, created_by int,
		updated_at ` + dttype + `, updated_by int
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE INDEX IDX_TAGS ON ` + tblPrefix + `TASKFLOW(tags)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE UNIQUE INDEX IDX_TF_UNIC_LIB ON ` + tblPrefix + `TASKFLOW(lib)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//task flow detail
	sql = `CREATE TABLE ` + tblPrefix + `TASKFLOWDETAIL (
		taskflowid int, idx int,
		taskid int,				
		nexttaskid_ok int,		
		nexttaskid_fail int,	
		retryif_fail int,
		primary key(taskflowid, idx)
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE UNIQUE INDEX IDX_PR_UNIC_LIB ON ` + tblPrefix + `PERIOD(lib)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//tache en cours
	sql = `CREATE TABLE ` + tblPrefix + `WIP (
		id INTEGER PRIMARY KEY,
		created_at ` + dttype + `,
		updated_at ` + dttype + `,
		state int,
		start_at ` + dttype + `,
		stop_at ` + dttype + `,
		msg varchar(500),
		scheduled int,
		agentid int
		)`

	return nil
}
