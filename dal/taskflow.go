package dal

import (
	"database/sql"
	"fmt"
	"time"
)

// TaskFlowList liste des taskflows
func TaskFlowList(filter SearchQuery) ([]DbTaskFlow, PagedResponse, error) {
	var err error
	arr := make([]DbTaskFlow, 0)
	arrMp := make(map[int]int) // id taskflow=idx arr
	var pagedResp PagedResponse

	//nb rows
	var nbRow sql.NullInt64
	if filter.Limit > 1 {
		q := ` SELECT count(*) as Nb FROM ` + tblPrefix + `TASKFLOW TASKFLOW ` + filter.GetSQLWhere()
		err = MainDB.QueryRow(q, filter.SQLParams...).Scan(&nbRow)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("TaskFlowList NbRow %w", err)
		}
	}

	//pour retour d'info avec info paging
	pagedResp = NewPagedResponse(arr, filter, int(nbRow.Int64))

	// listing
	q := ` SELECT TASKFLOW.id, TASKFLOW.lib, TASKFLOW.tags
	, TASKFLOW.activ, TASKFLOW.manuallaunch, TASKFLOW.scheduleid
	, TASKFLOW.err_management, TASKFLOW.queueid, TASKFLOW.last_start
	, TASKFLOW.last_stop, TASKFLOW.last_result, TASKFLOW.last_msg
	, TASKFLOW.named_args
	, USERC.login as loginC, TASKFLOW.created_at
	, USERU.login as loginU, TASKFLOW.updated_at	
	FROM ` + tblPrefix + `TASKFLOW TASKFLOW 
	left join  ` + tblPrefix + `USER USERC on USERC.id = TASKFLOW.created_by
	left join  ` + tblPrefix + `USER USERU on USERU.id = TASKFLOW.updated_by
	` + filter.GetSQLWhere()
	q = filter.AppendPaging(q, nbRow.Int64)

	rows, err := MainDB.Query(q, filter.SQLParams...)
	if err != nil {
		return nil, pagedResp, fmt.Errorf("TaskFlowList query %w", err)
	}
	defer rows.Close()
	var (
		id            int
		lib           sql.NullString
		tags          sql.NullString
		activ         sql.NullInt64
		manuallaunch  sql.NullInt64
		scheduleID    sql.NullInt64
		errManagement sql.NullInt64
		queueID       sql.NullInt64
		lastStart     sql.NullTime
		lastStop      sql.NullTime
		lastResult    sql.NullInt64
		lastMsg       sql.NullString
		namedArgs     sql.NullString
		createdAt     sql.NullTime
		updatedAt     sql.NullTime
		loginC        sql.NullString
		loginU        sql.NullString
	)

	for rows.Next() {
		err = rows.Scan(&id, &lib, &tags, &activ, &manuallaunch, &scheduleID, &errManagement,
			&queueID, &lastStart, &lastStop, &lastResult, &lastMsg, &namedArgs,
			&loginC, &createdAt, &loginU, &updatedAt)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("TaskFlowList scan %w", err)
		}
		arr = append(arr, DbTaskFlow{
			ID:           id,
			Lib:          lib.String,
			Tags:         splitIntFromStr(tags.String),
			Activ:        (activ.Int64 == 1),
			NamedArgs:    mapFromJSON(namedArgs.String),
			ManualLaunch: (manuallaunch.Int64 == 1),
			ScheduleID:   int(scheduleID.Int64),
			ErrMngt:      int(errManagement.Int64),
			QueueID:      int(queueID.Int64),
			LastStart:    lastStart.Time,
			LastStop:     lastStop.Time,
			LastResult:   int(lastResult.Int64),
			LastMsg:      lastMsg.String,
			Detail:       []DbTaskFlowDetail{},
			Info:         stdInfo(&loginC, &loginU, nil, &createdAt, &updatedAt, nil),
		})
		arrMp[id] = len(arr) - 1
	}
	if rows.Err() != nil && rows.Err() != sql.ErrNoRows {
		return nil, pagedResp, fmt.Errorf("TaskFlowList err %w", err)
	}

	//detail
	if len(arr) > 0 {
		idarr := make([]interface{}, len(arr))
		q = ` SELECT TASKFLOWDETAIL.taskflowid, TASKFLOWDETAIL.idx, TASKFLOWDETAIL.taskid, 
			TASKFLOWDETAIL.nexttaskid_ok, TASKFLOWDETAIL.nexttaskid_fail, TASKFLOWDETAIL.retryif_fail
			FROM ` + tblPrefix + `TASKFLOWDETAIL TASKFLOWDETAIL where TASKFLOWDETAIL.taskflowid in (0`
		for i := 0; i < len(arr); i++ {
			q += `,?`
			idarr[i] = arr[i].ID
		}
		q += `) order by TASKFLOWDETAIL.taskflowid, TASKFLOWDETAIL.idx`

		rowsDet, err := MainDB.Query(q, idarr...)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("TaskFlowList det query %w", err)
		}
		defer rowsDet.Close()
		var (
			taskflowid     int
			idx            int
			taskID         sql.NullInt64
			nextTaskIDOK   sql.NullInt64
			nextTaskIDFail sql.NullInt64
			retryIfFail    sql.NullInt64
		)
		for rowsDet.Next() {
			err = rowsDet.Scan(&taskflowid, &idx, &taskID, &nextTaskIDOK, &nextTaskIDFail, &retryIfFail)
			if err != nil {
				return nil, pagedResp, fmt.Errorf("TaskFlowList det scan %w", err)
			}
			arr[arrMp[taskflowid]].Detail = append(arr[arrMp[taskflowid]].Detail, DbTaskFlowDetail{
				Idx:            idx,
				TaskID:         int(taskID.Int64),
				NextTaskIDOK:   int(nextTaskIDOK.Int64),
				NextTaskIDFail: int(nextTaskIDFail.Int64),
				RetryIfFail:    int(retryIfFail.Int64),
			})
		}
		if rowsDet.Err() != nil && rowsDet.Err() != sql.ErrNoRows {
			return nil, pagedResp, fmt.Errorf("TaskFlowList det err %w", err)
		}
	}
	pagedResp.Data = arr

	return arr, pagedResp, nil
}

// TaskFlowGet get d'un taskflow
func TaskFlowGet(id int) (DbTaskFlow, error) {
	var ret DbTaskFlow
	filter := NewSearchQueryFromID("TASKFLOW", id)

	arr, _, err := TaskFlowList(filter)
	if err != nil {
		return ret, err
	}
	if len(arr) > 0 {
		ret = arr[0]
	}
	return ret, nil
}

// TaskFlowUpdate maj taskflow
func TaskFlowUpdate(elm DbTaskFlow, usrUpdater int, transaction bool) error {
	if transaction {
		_, err := MainDB.Exec(`BEGIN TRANSACTION`)
		if err != nil {
			return fmt.Errorf("TaskFlowUpdate err %w", err)
		}
		defer func() {
			MainDB.Exec(`ROLLBACK TRANSACTION`)
		}()
	}

	q := `UPDATE ` + tblPrefix + `TASKFLOW SET updated_by = ?, updated_at = ? 
		, lib = ?, tags = ? , activ = ?, manuallaunch = ?
		, scheduleid = ?, err_management = ?, queueid = ?, named_args = ?	
		where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elm.Lib, mergeIntToStr(elm.Tags),
		elm.Activ, elm.ManualLaunch, elm.ScheduleID, elm.ErrMngt, elm.QueueID,
		mapToJSON(&elm.NamedArgs), elm.ID)
	if err != nil {
		return fmt.Errorf("TaskFlowUpdate err %w", err)
	}

	//detail par delete/insert
	q = `DELETE FROM ` + tblPrefix + `TASKFLOWDETAIL where taskflowid = ? `
	_, err = MainDB.Exec(q, elm.ID)
	if err != nil {
		return fmt.Errorf("TaskFlowUpdate err %w", err)
	}

	q = `INSERT INTO ` + tblPrefix + `TASKFLOWDETAIL(taskflowid, idx, taskid, nexttaskid_ok, nexttaskid_fail
		, retryif_fail) VALUES (?,?,?,?,?,?)`
	for _, detail := range elm.Detail {
		_, err = MainDB.Exec(q, elm.ID, detail.Idx, detail.TaskID,
			detail.NextTaskIDOK, detail.NextTaskIDFail, detail.RetryIfFail)
		if err != nil {
			return fmt.Errorf("TaskFlowUpdate err %w", err)
		}
	}

	if transaction {
		_, err = MainDB.Exec(`COMMIT TRANSACTION`)
		if err != nil {
			return fmt.Errorf("TaskFlowUpdate err %w", err)
		}
	}

	return nil
}

// TaskFlowDelete flag taskflow suppression
func TaskFlowDelete(elmID int, usrUpdater int) error {
	_, err := MainDB.Exec(`BEGIN TRANSACTION`)
	if err != nil {
		return fmt.Errorf("TaskFlowDelete err %w", err)
	}
	defer func() {
		MainDB.Exec(`ROLLBACK TRANSACTION`)
	}()

	q := `DELETE FROM ` + tblPrefix + `TASKFLOWDETAIL where taskflowid = ? `
	_, err = MainDB.Exec(q, elmID)
	if err != nil {
		return fmt.Errorf("TaskFlowDelete err %w", err)
	}

	q = `DELETE FROM ` + tblPrefix + `TASKFLOW where id = ? `
	_, err = MainDB.Exec(q, elmID)
	if err != nil {
		return fmt.Errorf("TaskFlowDelete err %w", err)
	}

	_, err = MainDB.Exec(`COMMIT TRANSACTION`)
	if err != nil {
		return fmt.Errorf("TaskFlowDelete err %w", err)
	}

	return nil
}

// TaskFlowInsert insertion taskflow
func TaskFlowInsert(elm *DbTaskFlow, usrUpdater int) error {
	_, err := MainDB.Exec(`BEGIN TRANSACTION`)
	if err != nil {
		return fmt.Errorf("TaskFlowInsert err %w", err)
	}
	defer func() {
		MainDB.Exec(`ROLLBACK TRANSACTION`)
	}()

	//insert base
	q := `INSERT INTO ` + tblPrefix + `TASKFLOW (created_by, created_at) VALUES(?,?) `
	res, err := MainDB.Exec(q, usrUpdater, time.Now())
	if err != nil {
		return fmt.Errorf("TaskFlowInsert err %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("TaskFlowInsert err %w", err)
	}

	//mj pour le reste des champs
	elm.ID = int(id)
	err = TaskFlowUpdate(*elm, usrUpdater, false)
	if err != nil {
		return fmt.Errorf("TaskFlowInsert err %w", err)
	}

	_, err = MainDB.Exec(`COMMIT TRANSACTION`)
	if err != nil {
		return fmt.Errorf("TaskFlowInsert err %w", err)
	}
	return nil
}

// TaskFlowUpdate maj état d'un tf aprés exec
func TaskFlowUpdateLastState(taskflowId int, start, stop time.Time, result int, msg string) error {
	q := `UPDATE ` + tblPrefix + `TASKFLOW SET last_start = ?, last_stop = ? 
		, last_result = ?, last_msg = ?
		where id = ? `
	_, err := MainDB.Exec(q, start, stop, result, msg, taskflowId)
	if err != nil {
		return fmt.Errorf("TaskFlowUpdateLastState err %w", err)
	}
	return nil
}
