package dal

import (
	"database/sql"
	"fmt"
	"strconv"
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
		q := ` SELECT count(*) as Nb FROM ` + tblPrefix + `TASKFLOW ` + filter.GetSQLWhere()
		err = MainDB.QueryRow(q, filter.SQLParams...).Scan(&nbRow)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("TaskFlowList NbRow %w", err)
		}
	}

	//pour retour d'info avec info paging
	pagedResp = NewPagedResponse(arr, filter.Offset, filter.Limit, int(nbRow.Int64))

	// listing
	q := ` SELECT id, lib, tags, deleted_at FROM ` + tblPrefix + `TASKFLOW ` + filter.GetSQLWhere()
	q = filter.AppendPaging(q, nbRow.Int64)

	rows, err := MainDB.Query(q, filter.SQLParams...)
	if err != nil {
		return nil, pagedResp, fmt.Errorf("TaskFlowList query %w", err)
	}
	defer rows.Close()
	var (
		id        int
		lib       sql.NullString
		tags      sql.NullString
		deletedAt sql.NullTime
	)
	for rows.Next() {
		err = rows.Scan(&id, &lib, &tags, &deletedAt)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("TaskFlowList scan %w", err)
		}
		arr = append(arr, DbTaskFlow{
			ID:      id,
			Lib:     lib.String,
			Tags:    tags.String,
			Detail:  make([]DbTaskFlowDetail, 0),
			Deleted: deletedAt.Valid,
		})
		arrMp[id] = len(arr) - 1
	}
	if rows.Err() != nil && rows.Err() != sql.ErrNoRows {
		return nil, pagedResp, fmt.Errorf("TaskFlowList err %w", err)
	}

	//detail
	if len(arr) > 0 {
		idarr := make([]interface{}, len(arr))
		q = ` SELECT taskflowid, idx, taskid, nexttaskid_ok, nexttaskid_fail, notiffail
			FROM ` + tblPrefix + `TASKFLOWDETAIL where taskflowid in (0`
		for i := 0; i < len(arr); i++ {
			q += `,?`
			idarr[i] = arr[i].ID
		}
		q += `) order by taskflowid, idx`

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
			notifFail      sql.NullInt64
		)
		for rowsDet.Next() {
			err = rowsDet.Scan(&taskflowid, &idx, &taskID, &nextTaskIDOK, &nextTaskIDFail, &notifFail)
			if err != nil {
				return nil, pagedResp, fmt.Errorf("TaskFlowList det scan %w", err)
			}
			arr[arrMp[taskflowid]].Detail = append(arr[arrMp[taskflowid]].Detail, DbTaskFlowDetail{
				Idx:            idx,
				TaskID:         int(taskID.Int64),
				NextTaskIDOK:   int(nextTaskIDOK.Int64),
				NextTaskIDFail: int(nextTaskIDFail.Int64),
				NotifFail:      int(notifFail.Int64),
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
	filter := NewSearchQueryFromID(id)

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
	strDelQ := ""
	if !elm.Deleted {
		strDelQ = ", deleted_by = NULL, deleted_at = NULL"
	} else {
		strDelQ = ", deleted_by = " + strconv.Itoa(usrUpdater) + ", deleted_at = '" + time.Now().Format("2006-01-02T15:04:05.999") + "'"
	}

	if transaction {
		_, err := MainDB.Exec(`BEGIN TRANSACTION`)
		if err != nil {
			return fmt.Errorf("TaskFlowUpdate err %w", err)
		}
		defer func() {
			MainDB.Exec(`ROLLBACK TRANSACTION`)
		}()
	}

	q := `UPDATE ` + tblPrefix + `TASKFLOW SET
		updated_by = ?, updated_at = ? ` + strDelQ + `
		, lib = ?, tags = ? where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elm.Lib, elm.Tags, elm.ID)
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
		, notiffail) VALUES (?,?,?,?,?,?)`
	for _, detail := range elm.Detail {
		_, err = MainDB.Exec(q, elm.ID, detail.Idx, detail.TaskID,
			detail.NextTaskIDOK, detail.NextTaskIDFail, detail.NotifFail)
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
	q := `UPDATE ` + tblPrefix + `TASKFLOW SET deleted_by = ?, deleted_at = ? where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elmID)
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
