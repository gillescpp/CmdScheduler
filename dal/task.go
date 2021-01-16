package dal

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

// TaskList liste des tasks
func TaskList(filter SearchQuery) ([]DbTask, PagedResponse, error) {
	var err error
	arr := make([]DbTask, 0)
	var pagedResp PagedResponse

	//nb rows
	var nbRow sql.NullInt64
	if filter.Limit > 1 {
		q := ` SELECT count(*) as Nb FROM ` + tblPrefix + `TASK ` + filter.GetSQLWhere()
		err = MainDB.QueryRow(q, filter.SQLParams...).Scan(&nbRow)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("TaskList NbRow %w", err)
		}
	}

	//pour retour d'info avec info paging
	pagedResp = NewPagedResponse(arr, filter.Offset, filter.Limit, int(nbRow.Int64))

	// listing
	q := ` SELECT id, lib, type, timeout, log_store, cmd, args, start_in, exec_on, deleted_at
		FROM ` + tblPrefix + `TASK ` + filter.GetSQLWhere()
	q = filter.AppendPaging(q, nbRow.Int64)

	rows, err := MainDB.Query(q, filter.SQLParams...)
	if err != nil {
		return nil, pagedResp, fmt.Errorf("TaskList query %w", err)
	}
	defer rows.Close()
	var (
		id        int
		lib       sql.NullString
		ttype     sql.NullString
		timeout   sql.NullInt64
		logStore  sql.NullString
		cmd       sql.NullString
		args      sql.NullString
		startIn   sql.NullString
		execOn    sql.NullString
		deletedAt sql.NullTime
	)
	for rows.Next() {
		err = rows.Scan(&id, &lib, &ttype, &timeout, &logStore, &cmd, &args, &startIn, &execOn, &deletedAt)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("TaskList scan %w", err)
		}
		arr = append(arr, DbTask{
			ID:       id,
			Lib:      lib.String,
			Type:     ttype.String,
			Timeout:  int(timeout.Int64),
			LogStore: logStore.String,
			Cmd:      cmd.String,
			Args:     args.String,
			StartIn:  startIn.String,
			ExecOn:   execOn.String,
			Deleted:  deletedAt.Valid,
		})
	}
	if rows.Err() != nil && rows.Err() != sql.ErrNoRows {
		return nil, pagedResp, fmt.Errorf("TaskList err %w", err)
	}
	pagedResp.Data = arr

	return arr, pagedResp, nil
}

// TaskGet get d'un task
func TaskGet(id int) (DbTask, error) {
	var ret DbTask
	filter := NewSearchQueryFromID(id)

	arr, _, err := TaskList(filter)
	if err != nil {
		return ret, err
	}
	if len(arr) > 0 {
		ret = arr[0]
	}
	return ret, nil
}

// TaskUpdate maj task
func TaskUpdate(elm DbTask, usrUpdater int) error {
	strDelQ := ""
	if !elm.Deleted {
		strDelQ = ", deleted_by = NULL, deleted_at = NULL"
	} else {
		strDelQ = ", deleted_by = " + strconv.Itoa(usrUpdater) + ", deleted_at = '" + time.Now().Format("2006-01-02T15:04:05.999") + "'"
	}

	q := `UPDATE ` + tblPrefix + `TASK SET
		updated_by = ?, updated_at = ? ` + strDelQ + `
		, lib = ?, type = ?, timeout = ?, log_store = ?, cmd = ?, args = ?
		, start_in = ?, exec_on = ?
		where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elm.Lib, elm.Type, elm.Timeout, elm.LogStore,
		elm.Cmd, elm.Args, elm.StartIn, elm.ExecOn, elm.ID)
	if err != nil {
		return fmt.Errorf("TaskUpdate err %w", err)
	}

	return nil
}

// TaskDelete flag task suppression
func TaskDelete(elmID int, usrUpdater int) error {
	q := `UPDATE ` + tblPrefix + `TASK SET deleted_by = ?, deleted_at = ? where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elmID)
	if err != nil {
		return fmt.Errorf("TaskDelete err %w", err)
	}
	return nil
}

// TaskInsert insertion task
func TaskInsert(elm *DbTask, usrUpdater int) error {
	_, err := MainDB.Exec(`BEGIN TRANSACTION`)
	if err != nil {
		return fmt.Errorf("TaskInsert err %w", err)
	}
	defer func() {
		MainDB.Exec(`ROLLBACK TRANSACTION`)
	}()

	//insert base
	q := `INSERT INTO ` + tblPrefix + `TASK (created_by, created_at) VALUES(?,?) `
	res, err := MainDB.Exec(q, usrUpdater, time.Now())
	if err != nil {
		return fmt.Errorf("TaskInsert err %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("TaskInsert err %w", err)
	}

	//mj pour le reste des champs
	elm.ID = int(id)
	err = TaskUpdate(*elm, usrUpdater)
	if err != nil {
		return fmt.Errorf("TaskInsert err %w", err)
	}

	_, err = MainDB.Exec(`COMMIT TRANSACTION`)
	if err != nil {
		return fmt.Errorf("TaskInsert err %w", err)
	}
	return nil
}
