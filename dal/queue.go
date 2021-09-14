package dal

import (
	"database/sql"
	"fmt"
	"time"
)

// QueueList liste des queues
func QueueList(filter SearchQuery) ([]DbQueue, PagedResponse, error) {
	var err error
	arr := make([]DbQueue, 0)
	var pagedResp PagedResponse

	//nb rows
	var nbRow sql.NullInt64
	if filter.Limit > 1 {
		q := ` SELECT count(*) as Nb FROM ` + tblPrefix + `QUEUE QUEUE ` + filter.GetSQLWhere()
		err = MainDB.QueryRow(q, filter.SQLParams...).Scan(&nbRow)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("QueueList NbRow %w", err)
		}
	}

	//pour retour d'info avec info paging
	pagedResp = NewPagedResponse(arr, filter, int(nbRow.Int64))

	// listing
	q := ` SELECT QUEUE.id, QUEUE.lib, QUEUE.size, QUEUE.timeout, QUEUE.pausedfrom 
		, QUEUE.noexecwhile_queuelist
		, USERC.login as loginC, QUEUE.created_at
		, USERU.login as loginU, QUEUE.updated_at
		FROM ` + tblPrefix + `QUEUE QUEUE 
		left join  ` + tblPrefix + `USER USERC on USERC.id = QUEUE.created_by
		left join  ` + tblPrefix + `USER USERU on USERU.id = QUEUE.updated_by		
		` + filter.GetSQLWhere()
	q = filter.AppendPaging(q, nbRow.Int64)

	rows, err := MainDB.Query(q, filter.SQLParams...)
	if err != nil {
		return nil, pagedResp, fmt.Errorf("QueueList query %w", err)
	}
	defer rows.Close()
	var (
		id         int
		lib        sql.NullString
		size       sql.NullInt64
		timeout    sql.NullInt64
		pausedFrom sql.NullTime
		noexecQL   sql.NullString
		createdAt  sql.NullTime
		updatedAt  sql.NullTime
		loginC     sql.NullString
		loginU     sql.NullString
	)
	for rows.Next() {
		err = rows.Scan(&id, &lib, &size, &timeout, &pausedFrom, &noexecQL, &loginC, &createdAt, &loginU, &updatedAt)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("QueueList scan %w", err)
		}
		arr = append(arr, DbQueue{
			ID:               id,
			Lib:              lib.String,
			MaxSize:          int(size.Int64),
			MaxDuration:      int(timeout.Int64),
			PausedManual:     pausedFrom.Valid && !pausedFrom.Time.IsZero(),
			PausedManualFrom: pausedFrom.Time,
			NoExecWhile:      splitIntFromStr(noexecQL.String),
			Info:             stdInfo(&loginC, &loginU, nil, &createdAt, &updatedAt, nil),
		})
	}
	if rows.Err() != nil && rows.Err() != sql.ErrNoRows {
		return nil, pagedResp, fmt.Errorf("QueueList err %w", err)
	}
	pagedResp.Data = arr

	return arr, pagedResp, nil
}

// QueueGet get d'un queue
func QueueGet(id int) (DbQueue, error) {
	var ret DbQueue
	filter := NewSearchQueryFromID("QUEUE", id)

	arr, _, err := QueueList(filter)
	if err != nil {
		return ret, err
	}
	if len(arr) > 0 {
		ret = arr[0]
	}
	return ret, nil
}

// QueueUpdate maj queue
func QueueUpdate(elm DbQueue, usrUpdater int, admin bool) error {
	var pausedfrom sql.NullTime
	if elm.PausedManual {
		if elm.PausedManualFrom.IsZero() {
			elm.PausedManualFrom = time.Now()
		}
		pausedfrom.Time = elm.PausedManualFrom
		pausedfrom.Valid = true
	}
	q := `UPDATE ` + tblPrefix + `QUEUE SET
		updated_by = ?, updated_at = ? 
		, lib = ?, size = ?, timeout = ?, pausedfrom= ?, noexecwhile_queuelist = ?
		where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elm.Lib, elm.MaxSize, elm.MaxDuration,
		pausedfrom, mergeIntToStr(elm.NoExecWhile), elm.ID)
	if err != nil {
		return fmt.Errorf("QueueUpdate err %w", err)
	}

	return nil
}

// QueueDelete suppression
func QueueDelete(elmID int, usrUpdater int) error {
	q := `DELETE FROM ` + tblPrefix + `QUEUE where id = ? `
	_, err := MainDB.Exec(q, elmID)
	if err != nil {
		return fmt.Errorf("QueueDelete err %w", err)
	}
	return nil
}

// QueueInsert insertion queue
func QueueInsert(elm *DbQueue, usrUpdater int) error {
	_, err := MainDB.Exec(`BEGIN TRANSACTION`)
	if err != nil {
		return fmt.Errorf("QueueInsert err %w", err)
	}
	defer func() {
		MainDB.Exec(`ROLLBACK TRANSACTION`)
	}()

	//insert base
	q := `INSERT INTO ` + tblPrefix + `QUEUE (created_by, created_at) VALUES(?,?) `
	res, err := MainDB.Exec(q, usrUpdater, time.Now())
	if err != nil {
		return fmt.Errorf("QueueInsert err %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("QueueInsert err %w", err)
	}

	//mj pour le reste des champs
	elm.ID = int(id)
	err = QueueUpdate(*elm, usrUpdater, true)
	if err != nil {
		return fmt.Errorf("QueueInsert err %w", err)
	}

	_, err = MainDB.Exec(`COMMIT TRANSACTION`)
	if err != nil {
		return fmt.Errorf("QueueInsert err %w", err)
	}
	return nil
}
