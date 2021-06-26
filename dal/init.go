package dal

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	//sqlite
	_ "github.com/mattn/go-sqlite3"
)

//instance globale bdd
var (
	MainDB    *sql.DB
	tblPrefix string
	dbSchema  string
	dbDriver  string
)

// InitDb prepa bdd mais
func InitDb(driver string, datasource string, schema string) error {
	//conn/création
	dbSchema = strings.ReplaceAll(strings.TrimSpace(schema), " ", "-")
	dbDriver = driver
	tblPrefix = schema + "."
	if strings.EqualFold(dbDriver, "sqlite3") {
		dbSchema = ""
		tblPrefix = ""
	}

	var err error
	MainDB, err = sql.Open(dbDriver, datasource)
	if err != nil {
		return fmt.Errorf("Open DB : %w", err)
	}
	return initDbTables()
}

// updDbVersion init/set table version
func updDbVersion(dbversion *int) error {
	var err error
	//init table config kv
	if *dbversion <= 0 {
		if strings.EqualFold(dbDriver, "mssql") {
			initKv := `IF EXISTS (select 1 from INFORMATION_SCHEMA.TABLES
			where TABLE_SCHEMA = ? AND TABLE_NAME = ?) 
			CREATE TABLE ` + tblPrefix + `CFG (KID varchar(100), KVAL varchar(1000), PRIMARY KEY(KID)) `
			_, err = MainDB.Exec(initKv, dbSchema, tblPrefix+"CFG")
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
		log.Println("Upgrade db q", newVersion, "...")
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

	autoinc := "INTEGER NOT NULL IDENTITY"
	if strings.EqualFold(dbDriver, "sqlite3") {
		autoinc = "INTEGER PRIMARY KEY"
	}

	// note : protection des nom de colonne non portable `` pour mysql, [] pour mssql
	// donc pas d'espace dans les nom de champs et table
	// rajout de versionedDML a faire en dessous des autres

	// USER, rightlevel indique un niveau de droit (system de droit basique)
	iv := 1
	sql := `CREATE ` + tblPrefix + `TABLE USER (
		id ` + autoinc + `,
		name VARCHAR(150),
		login VARCHAR(60),
		rightlevel int,	
		password VARCHAR(60),
		deleted_at datetime, deleted_by int,
		created_at datetime, created_by int,
		updated_at datetime, updated_by int
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE UNIQUE INDEX IDX_CLI_UNIC_LOGIN ON ` + tblPrefix + `USER(login)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE INDEX IDX_CLI_DELETED ON ` + tblPrefix + `USER(deleted_at)`
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
		}, 0)
	}

	// agents d'exec
	sql = `CREATE TABLE ` + tblPrefix + `AGENT (
		id ` + autoinc + `,
		host VARCHAR(150),  
		apikey VARCHAR(260),
		certsignallowed VARCHAR(512),
		tls int,
		deleted_at datetime, deleted_by int,
		created_at datetime, created_by int,
		updated_at datetime, updated_by int
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
			cmd VARCHAR(250),
			args VARCHAR(250),
			start_in VARCHAR(250),
			exec_on VARCHAR(250), -- liste des agents autorisé pour l'exec
			deleted_at datetime, deleted_by int,
			created_at datetime, created_by int,
			updated_at datetime, updated_by int
			)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil { /////////////////// todo virer les com dans les code sql
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE INDEX IDX_TASK_DELETED ON ` + tblPrefix + `TASK(deleted_at)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//task flow head
	sql = `CREATE TABLE ` + tblPrefix + `TASKFLOW (
		id ` + autoinc + `,
		lib VARCHAR(100),
		tags VARCHAR(100),
		deleted_at datetime, deleted_by int,
		created_at datetime, created_by int,
		updated_at datetime, updated_by int
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE INDEX IDX_TASKFLOW_DELETED ON ` + tblPrefix + `TASKFLOW(deleted_at)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}
	sql = `CREATE INDEX IDX_TASKFLOW_LIB ON ` + tblPrefix + `TASKFLOW(lib, tags)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//TODO : rajouter un idx unique sur lib

	//task flow detail TODO : rajouter un compteur de passe max (nombre de fois ou l'action peut être appelé, pour des schemas de ressais limité en nnombre)
	sql = `CREATE TABLE ` + tblPrefix + `TASKFLOWDETAIL (
		taskflowid int, idx int,
		taskid int,				-- tache a executer
		nexttaskid_ok int,		-- -1=end, 0=next, 1+=goto idx 1+ 
		nexttaskid_fail int,	-- -1=end, 0=next, 1+=goto idx 1+ 
		notiffail int,			-- 0=non, 1=oui
		primary key(taskflowid, idx)
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//schedule : 1 schedule -> lance 1 taskflow
	sql = `CREATE TABLE ` + tblPrefix + `SCHED (
		id ` + autoinc + `,
		taskflowid int,			-- taskflow lancé
		err_level int,
		queueid int,			-- eventuel queue
		activ int,				-- 1=oui, autre =non
		last_start datetime,
		last_stop datetime,
		last_result int,		-- 1=ok, 0=err
		last_msg varchar(500),	-- dernier err constaté
		deleted_at datetime, deleted_by int,
		created_at datetime, created_by int,
		updated_at datetime, updated_by int
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE INDEX IDX_SCHED_DELETED ON ` + tblPrefix + `SCHED(deleted_at)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}
	sql = `CREATE INDEX IDX_SCHED_TFID ON ` + tblPrefix + `SCHED(taskflowid)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//sched détail
	sql = `CREATE TABLE ` + tblPrefix + `SCHEDDETAIL (
		schedid int, idx int,
		interval int,			-- intervalle en secondes
		intervalhours varchar(500),		-- plages horaires 08:00:05-10:00:00,14:00:00-18:00:00
		hours varchar(500),		-- liste horaire d'exec 08:00:05, 10:00:00
		months varchar(12),		-- mois d'exex format JFMAMJJASOND : "01000100000" ou "*" pour tous
		weekdays varchar(7),	-- jours d'exex format LMMJVSD : "1111100" ou "*" pour tous
		monthdays varchar(100),	-- jours du mois sous forme de n° : "1,15", et ou code "1MON, 2TUE, FIRST, LAST" 
								-- (1er lundi du mois, 2eme mardi du mois, 1e j du mois, dernier j du mois) ou "*" pour tous
		primary key(schedid, idx)
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//queue (liste de taskflow a executer)
	sql = `CREATE TABLE ` + tblPrefix + `QUEUE (
		id ` + autoinc + `,
		lib VARCHAR(100),
		size int,				-- max element dans la queue
		timeout int,			-- durée max d'une exec au sein de cette queue
		deleted_at datetime, deleted_by int,
		created_at datetime, created_by int,
		updated_at datetime, updated_by int
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE UNIQUE INDEX IDX_QUEUE_LIB ON ` + tblPrefix + `QUEUE (lib)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//tags
	sql = `CREATE TABLE ` + tblPrefix + `TAG (
		id ` + autoinc + `,
		lib VARCHAR(50),
		tgroup VARCHAR(50),
		deleted_at datetime, deleted_by int,
		created_at datetime, created_by int,
		updated_at datetime, updated_by int
		)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE UNIQUE INDEX IDX_TAG_LIB ON ` + tblPrefix + `TAG (lib, tgroup)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	// ajout time zone
	sql = `ALTER TABLE ` + tblPrefix + `SCHED ADD time_zone varchar(50)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	//////////////////TODO besoin d'un état des tache en cours (id des travaux en cours sur les agents..., schedule ephemere des instant start...)
	///todo : code coul pour les tags et groupe de tags, ordre tags ?

	return nil
}
