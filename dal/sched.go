package dal

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

// SchedList liste des scheds
func SchedList(filter SearchQuery) ([]DbSched, PagedResponse, error) {
	var err error
	arr := make([]DbSched, 0)
	arrMp := make(map[int]int) // id task=idx arr
	var pagedResp PagedResponse

	//nb rows
	var nbRow sql.NullInt64
	if filter.Limit > 1 {
		q := ` SELECT count(*) as Nb FROM ` + tblPrefix + `SCHED ` + filter.GetSQLWhere()
		err = MainDB.QueryRow(q, filter.SQLParams...).Scan(&nbRow)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("SchedList NbRow %w", err)
		}
	}

	//pour retour d'info avec info paging
	pagedResp = NewPagedResponse(arr, filter.Offset, filter.Limit, int(nbRow.Int64))

	// listing
	q := ` SELECT id, taskflowid, err_level, queueid, activ, 
	last_start, last_stop, last_result, last_msg, deleted_at
		FROM ` + tblPrefix + `SCHED ` + filter.GetSQLWhere()
	q = filter.AppendPaging(q, nbRow.Int64)

	rows, err := MainDB.Query(q, filter.SQLParams...)
	if err != nil {
		return nil, pagedResp, fmt.Errorf("SchedList query %w", err)
	}
	defer rows.Close()
	var (
		id         int
		taskFlowID sql.NullInt64
		errLevel   sql.NullInt64
		queueID    sql.NullInt64
		activ      sql.NullInt64
		lastStart  sql.NullTime
		lastStop   sql.NullTime
		lastResult sql.NullInt64
		lastMsg    sql.NullString
		deletedAt  sql.NullTime
	)
	for rows.Next() {
		err = rows.Scan(&id, &taskFlowID, &errLevel, &queueID, &activ,
			&lastStart, &lastStop, &lastResult, &lastMsg, &deletedAt)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("SchedList scan %w", err)
		}
		arr = append(arr, DbSched{
			ID:         id,
			TaskFlowID: int(taskFlowID.Int64),
			ErrLevel:   int(errLevel.Int64),
			QueueID:    int(queueID.Int64),
			Activ:      (activ.Int64 > 0),
			LastStart:  lastStart.Time,
			LastStop:   lastStop.Time,
			LastResult: int(lastResult.Int64),
			LastMsg:    lastMsg.String,
			Detail:     make([]DbSchedDetail, 0),
			Deleted:    deletedAt.Valid,
		})
		arrMp[id] = len(arr) - 1
	}
	if rows.Err() != nil && rows.Err() != sql.ErrNoRows {
		return nil, pagedResp, fmt.Errorf("SchedList err %w", err)
	}

	//detail
	if len(arr) > 0 {
		idarr := make([]interface{}, len(arr))
		q = ` SELECT schedid, interval, intervalhours, hours, 
			months, weekdays, monthdays
			FROM ` + tblPrefix + `SCHEDDETAIL where schedid in (0`
		for i := 0; i < len(arr); i++ {
			q += `,?`
			idarr[i] = arr[i].ID
		}
		q += `) order by schedid, idx`

		rowsDet, err := MainDB.Query(q, idarr...)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("SchedList det query %w", err)
		}
		defer rowsDet.Close()
		var (
			schedid       int
			interval      sql.NullInt64
			intervalhours sql.NullString
			hours         sql.NullString
			months        sql.NullString
			weekdays      sql.NullString
			monthdays     sql.NullString
		)
		for rowsDet.Next() {
			err = rowsDet.Scan(&schedid, &interval, &intervalhours,
				&hours, &months, &weekdays, &monthdays)
			if err != nil {
				return nil, pagedResp, fmt.Errorf("SchedList det scan %w", err)
			}
			arr[arrMp[schedid]].Detail = append(arr[arrMp[schedid]].Detail, DbSchedDetail{
				Interval:      int(interval.Int64),
				IntervalHours: intervalhours.String,
				Hours:         hours.String,
				Months:        months.String,
				WeekDays:      weekdays.String,
				MonthDays:     monthdays.String,
			})
		}
		if rowsDet.Err() != nil && rowsDet.Err() != sql.ErrNoRows {
			return nil, pagedResp, fmt.Errorf("SchedList det err %w", err)
		}
	}
	//validation pour renseigner les attributs de travail
	for e := range arr {
		arr[e].Validate(false)
	}
	pagedResp.Data = arr

	return arr, pagedResp, nil
}

// SchedGet get d'un sched
func SchedGet(id int) (DbSched, error) {
	var ret DbSched
	filter := NewSearchQueryFromID(id)

	arr, _, err := SchedList(filter)
	if err != nil {
		return ret, err
	}
	if len(arr) > 0 {
		ret = arr[0]
	}
	return ret, nil
}

// SchedUpdate maj sched
func SchedUpdate(elm DbSched, usrUpdater int, transaction bool) error {
	strDelQ := ""
	if !elm.Deleted {
		strDelQ = ", deleted_by = NULL, deleted_at = NULL"
	} else {
		strDelQ = ", deleted_by = " + strconv.Itoa(usrUpdater) + ", deleted_at = '" + time.Now().Format("2006-01-02T15:04:05.999") + "'"
	}

	if transaction {
		_, err := MainDB.Exec(`BEGIN TRANSACTION`)
		if err != nil {
			return fmt.Errorf("SchedUpdate err %w", err)
		}
		defer func() {
			MainDB.Exec(`ROLLBACK TRANSACTION`)
		}()
	}

	activ := 0
	if elm.Activ && !elm.Deleted {
		activ = 1
	}
	q := `UPDATE ` + tblPrefix + `SCHED SET
		updated_by = ?, updated_at = ? ` + strDelQ + `
		, taskflowid = ?, err_level = ?, queueid = ?, activ = ?
		where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elm.TaskFlowID, elm.ErrLevel, elm.QueueID, activ, elm.ID)
	if err != nil {
		return fmt.Errorf("SchedUpdate err %w", err)
	}

	//detail par delete/insert
	q = `DELETE FROM ` + tblPrefix + `SCHEDDETAIL where schedid = ? `
	_, err = MainDB.Exec(q, elm.ID)
	if err != nil {
		return fmt.Errorf("SchedUpdate err %w", err)
	}

	q = `INSERT INTO ` + tblPrefix + `SCHEDDETAIL(schedid, idx, interval, intervalhours
		, hours, months, weekdays, monthdays) VALUES (?,?,?,?,?,?,?,?)`
	for i, detail := range elm.Detail {
		_, err = MainDB.Exec(q, elm.ID, i, detail.Interval, detail.IntervalHours, detail.Hours,
			detail.Months, detail.WeekDays, detail.MonthDays)
		if err != nil {
			return fmt.Errorf("SchedUpdate err %w", err)
		}
	}

	if transaction {
		_, err = MainDB.Exec(`COMMIT TRANSACTION`)
		if err != nil {
			return fmt.Errorf("SchedUpdate err %w", err)
		}
	}

	return nil
}

// SchedDelete flag sched suppression
func SchedDelete(elmID int, usrUpdater int) error {
	q := `UPDATE ` + tblPrefix + `SCHED SET deleted_by = ?, deleted_at = ? where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elmID)
	if err != nil {
		return fmt.Errorf("SchedDelete err %w", err)
	}
	return nil
}

// SchedInsert insertion sched
func SchedInsert(elm *DbSched, usrUpdater int) error {
	_, err := MainDB.Exec(`BEGIN TRANSACTION`)
	if err != nil {
		return fmt.Errorf("SchedInsert err %w", err)
	}
	defer func() {
		MainDB.Exec(`ROLLBACK TRANSACTION`)
	}()

	//insert base
	q := `INSERT INTO ` + tblPrefix + `SCHED (created_by, created_at) VALUES(?,?) `
	res, err := MainDB.Exec(q, usrUpdater, time.Now())
	if err != nil {
		return fmt.Errorf("SchedInsert err %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("SchedInsert err %w", err)
	}

	//mj pour le reste des champs
	elm.ID = int(id)
	err = SchedUpdate(*elm, usrUpdater, false)
	if err != nil {
		return fmt.Errorf("SchedInsert err %w", err)
	}

	_, err = MainDB.Exec(`COMMIT TRANSACTION`)
	if err != nil {
		return fmt.Errorf("SchedInsert err %w", err)
	}

	return nil
}

// SchedBegin maj sched date start
func SchedBegin(schedID int) error {
	q := `UPDATE ` + tblPrefix + `SCHED SET last_start = ?, last_stop = NULL
		, last_result = NULL, last_msg = NULL
		where id = ? `
	_, err := MainDB.Exec(q, time.Now(), schedID)
	if err != nil {
		return fmt.Errorf("SchedBegin err %w", err)
	}
	return nil
}

// SchedEnd maj sched date fin
func SchedEnd(schedID int, resultCode int, resultMsg string) error {
	q := `UPDATE ` + tblPrefix + `SCHED SET last_stop = ?
		, last_result = ?, last_msg = ?
		where id = ? `
	_, err := MainDB.Exec(q, time.Now(), resultCode, resultMsg, schedID)
	if err != nil {
		return fmt.Errorf("SchedEnd err %w", err)
	}

	return nil
}
