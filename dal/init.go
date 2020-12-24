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
	iv := 1
	sql := `CREATE ` + tblPrefix + `TABLE USER (
		id ` + autoinc + `,
		name VARCHAR(150),
		login VARCHAR(60),
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

	sql = `CREATE TABLE ` + tblPrefix + `TASK (
			id ` + autoinc + `,
			type VARCHAR(20),
			timeout INT,
			log_store VARCHAR(50),
			cmd VARCHAR(250),
			args VARCHAR(250),
			start_in VARCHAR(250),
			deleted_at datetime, deleted_by int,
			created_at datetime, created_by int,
			updated_at datetime, updated_by int
			)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	sql = `CREATE UNIQUE INDEX IDX_TASK_DELETED ON ` + tblPrefix + `TASK(deleted_at)`
	if iv, err = (iv + 1), versionedDML(iv, &curVersion, sql); err != nil {
		return fmt.Errorf("initDbTables %v %w", iv, err)
	}

	return nil
}
