package dal

import (
	"database/sql"
	"fmt"
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
		q := ` SELECT count(*) as Nb FROM ` + tblPrefix + `TAG TAG ` + filter.GetSQLWhere()
		err = MainDB.QueryRow(q, filter.SQLParams...).Scan(&nbRow)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("TagList NbRow %w", err)
		}
	}

	//pour retour d'info avec info paging
	pagedResp = NewPagedResponse(arr, filter, int(nbRow.Int64))

	// listing
	q := ` SELECT TAG.id, TAG.lib, TAG.tgroup
		, USERC.login as loginC, TAG.created_at
		, USERU.login as loginU, TAG.updated_at
		FROM ` + tblPrefix + `TAG TAG 
		left join  ` + tblPrefix + `USR USERC on USERC.id = TAG.created_by
		left join  ` + tblPrefix + `USR USERU on USERU.id = TAG.updated_by
		` + filter.GetSQLWhere()
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
		createdAt sql.NullTime
		updatedAt sql.NullTime
		loginC    sql.NullString
		loginU    sql.NullString
	)
	for rows.Next() {
		err = rows.Scan(&id, &lib, &group, &loginC, &createdAt, &loginU, &updatedAt)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("TagList scan %w", err)
		}
		arr = append(arr, DbTag{
			ID:    id,
			Lib:   lib.String,
			Group: group.String,
			Info:  stdInfo(&loginC, &loginU, nil, &createdAt, &updatedAt, nil),
		})
	}
	if rows.Err() != nil && rows.Err() != sql.ErrNoRows {
		return nil, pagedResp, fmt.Errorf("TagList err %w", err)
	}
	pagedResp.Data = arr

	return arr, pagedResp, nil
}

// TagGet get d'un tag
func TagGet(id int) (DbTag, error) {
	var ret DbTag
	filter := NewSearchQueryFromID("TAG", id)

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
func TagUpdate(elm DbTag, usrUpdater int, tx *sql.Tx) error {
	q := `UPDATE ` + tblPrefix + `TAG SET
		updated_by = ?, updated_at = ? 
		, lib = ?, tgroup = ?
		where id = ? `
	_, err := TxExec(tx, q, usrUpdater, time.Now(), elm.Lib, elm.Group, elm.ID)
	if err != nil {
		return fmt.Errorf("TagUpdate err %w", err)
	}

	return nil
}

// TagDelete flag tag suppression
func TagDelete(elmID int, usrUpdater int) error {
	q := `DELETE FROM ` + tblPrefix + `TAG where id = ? `
	_, err := TxExec(nil, q, elmID)
	if err != nil {
		return fmt.Errorf("TagDelete err %w", err)
	}
	return nil
}

// TagInsert insertion tag
func TagInsert(elm *DbTag, usrUpdater int) error {
	tx, err := MainDB.Begin()
	if err != nil {
		return fmt.Errorf("TagInsert err %w", err)
	}
	defer tx.Rollback()

	//insert base
	q := `INSERT INTO ` + tblPrefix + `TAG (created_by, created_at) VALUES(?,?) `
	id, err := TxInsert(tx, q, usrUpdater, time.Now())
	if err != nil {
		return fmt.Errorf("TagInsert err %w", err)
	}

	//mj pour le reste des champs
	elm.ID = int(id)
	err = TagUpdate(*elm, usrUpdater, tx)
	if err != nil {
		return fmt.Errorf("TagInsert err %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("TagInsert err %w", err)
	}
	return nil
}
