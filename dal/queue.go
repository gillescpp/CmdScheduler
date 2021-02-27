package dal

import (
	"database/sql"
	"fmt"
	"strconv"
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
		q := ` SELECT count(*) as Nb FROM ` + tblPrefix + `QUEUE ` + filter.GetSQLWhere()
		err = MainDB.QueryRow(q, filter.SQLParams...).Scan(&nbRow)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("QueueList NbRow %w", err)
		}
	}

	//pour retour d'info avec info paging
	pagedResp = NewPagedResponse(arr, filter.Offset, filter.Limit, int(nbRow.Int64))

	// listing
	q := ` SELECT id, lib, size, timeout, deleted_at
		FROM ` + tblPrefix + `QUEUE ` + filter.GetSQLWhere()
	q = filter.AppendPaging(q, nbRow.Int64)

	rows, err := MainDB.Query(q, filter.SQLParams...)
	if err != nil {
		return nil, pagedResp, fmt.Errorf("QueueList query %w", err)
	}
	defer rows.Close()
	var (
		id        int
		lib       sql.NullString
		size      sql.NullInt64
		timeout   sql.NullInt64
		deletedAt sql.NullTime
	)
	for rows.Next() {
		err = rows.Scan(&id, &lib, &size, &timeout, &deletedAt)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("QueueList scan %w", err)
		}
		arr = append(arr, DbQueue{
			ID:      id,
			Lib:     lib.String,
			Size:    int(size.Int64),
			Timeout: int(timeout.Int64),
			Deleted: deletedAt.Valid,
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
	filter := NewSearchQueryFromID(id)

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
	strDelQ := ""
	if admin {
		if !elm.Deleted {
			strDelQ = ", deleted_by = NULL, deleted_at = NULL"
		} else {
			strDelQ = ", deleted_by = " + strconv.Itoa(usrUpdater) + ", deleted_at = '" + time.Now().Format("2006-01-02T15:04:05.999") + "'"
		}
	}

	q := `UPDATE ` + tblPrefix + `QUEUE SET
		updated_by = ?, updated_at = ? ` + strDelQ + `
		, lib = ?, size = ?, timeout = ?
		where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elm.Lib, elm.Size, elm.Timeout, elm.ID)
	if err != nil {
		return fmt.Errorf("QueueUpdate err %w", err)
	}

	return nil
}

// QueueDelete flag queue suppression
func QueueDelete(elmID int, usrUpdater int) error {
	q := `UPDATE ` + tblPrefix + `QUEUE SET deleted_by = ?, deleted_at = ? where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elmID)
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
