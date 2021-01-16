package dal

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

// TagList liste des tags
func TagList(filter SearchQuery) ([]DbTag, PagedResponse, error) {
	var err error
	arr := make([]DbTag, 0)
	var pagedResp PagedResponse

	//nb rows
	var nbRow sql.NullInt64
	if filter.Limit > 1 {
		q := ` SELECT count(*) as Nb FROM ` + tblPrefix + `TAG ` + filter.GetSQLWhere()
		err = MainDB.QueryRow(q, filter.SQLParams...).Scan(&nbRow)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("TagList NbRow %w", err)
		}
	}

	//pour retour d'info avec info paging
	pagedResp = NewPagedResponse(arr, filter.Offset, filter.Limit, int(nbRow.Int64))

	// listing
	q := ` SELECT id, lib, tgroup, deleted_at
		FROM ` + tblPrefix + `TAG ` + filter.GetSQLWhere()
	q = filter.AppendPaging(q, nbRow.Int64)

	rows, err := MainDB.Query(q, filter.SQLParams...)
	if err != nil {
		return nil, pagedResp, fmt.Errorf("TagList query %w", err)
	}
	defer rows.Close()
	var (
		id        int
		lib       sql.NullString
		group     sql.NullString
		deletedAt sql.NullTime
	)
	for rows.Next() {
		err = rows.Scan(&id, &lib, &group, &deletedAt)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("TagList scan %w", err)
		}
		arr = append(arr, DbTag{
			ID:      id,
			Lib:     lib.String,
			Group:   group.String,
			Deleted: !deletedAt.Valid,
		})
	}
	if rows.Err() != nil && rows.Err() != sql.ErrNoRows {
		return nil, pagedResp, fmt.Errorf("TagList err %w", err)
	}

	return arr, pagedResp, nil
}

// TagGet get d'un tag
func TagGet(id int) (DbTag, error) {
	var ret DbTag
	filter := NewSearchQueryFromID(id)

	arr, _, err := TagList(filter)
	if err != nil {
		return ret, err
	}
	if len(arr) > 0 {
		ret = arr[0]
	}
	return ret, nil
}

// TagUpdate maj tag
func TagUpdate(elm DbTag, usrUpdater int) error {
	strDelQ := ""
	if !elm.Deleted {
		strDelQ = ", deleted_by = NULL, deleted_at = NULL"
	} else {
		strDelQ = ", deleted_by = " + strconv.Itoa(usrUpdater) + ", deleted_at = '" + time.Now().Format("2006-01-02T15:04:05.999") + "'"
	}

	q := `UPDATE ` + tblPrefix + `TAG SET
		updated_by = ?, updated_at = ? ` + strDelQ + `
		, lib = ?, tgroup = ?
		where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elm.Lib, elm.Group, elm.ID)
	if err != nil {
		return fmt.Errorf("TagUpdate err %w", err)
	}

	return nil
}

// TagDelete flag tag suppression
func TagDelete(elmID int, usrUpdater int) error {
	q := `UPDATE ` + tblPrefix + `TAG SET deleted_by = ?, deleted_at = ? where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elmID)
	if err != nil {
		return fmt.Errorf("TagDelete err %w", err)
	}
	return nil
}

// TagInsert insertion tag
func TagInsert(elm *DbTag, usrUpdater int) error {
	_, err := MainDB.Exec(`BEGIN TRANSACTION`)
	if err != nil {
		return fmt.Errorf("TagInsert err %w", err)
	}
	defer func() {
		MainDB.Exec(`ROLLBACK TRANSACTION`)
	}()

	//insert base
	q := `INSERT INTO ` + tblPrefix + `TAG (created_by, created_at) VALUES(?,?) `
	res, err := MainDB.Exec(q, usrUpdater, time.Now())
	if err != nil {
		return fmt.Errorf("TagInsert err %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("TagInsert err %w", err)
	}

	//mj pour le reste des champs
	elm.ID = int(id)
	err = TagUpdate(*elm, usrUpdater)
	if err != nil {
		return fmt.Errorf("TagInsert err %w", err)
	}

	_, err = MainDB.Exec(`COMMIT TRANSACTION`)
	if err != nil {
		return fmt.Errorf("TagInsert err %w", err)
	}
	return nil
}
