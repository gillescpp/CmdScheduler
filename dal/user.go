package dal

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// UserList liste des users
func UserList(filter SearchQuery) ([]DbUser, PagedResponse, error) {
	var err error
	arr := make([]DbUser, 0)
	var pagedResp PagedResponse

	//nb rows
	var nbRow sql.NullInt64
	if filter.Limit > 1 {
		q := ` SELECT count(*) as Nb FROM ` + tblPrefix + `USR USR ` + filter.GetSQLWhere()
		err = MainDB.QueryRow(q, filter.SQLParams...).Scan(&nbRow)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("UserList NbRow %w", err)
		}
	}

	//pour retour d'info avec info paging
	pagedResp = NewPagedResponse(arr, filter, int(nbRow.Int64))

	// listing
	q := ` SELECT USR.id, USR.name, USR.login, USR.password, USR.rightlevel
		, USERC.login as loginC, USR.created_at
		, USERU.login as loginU, USR.updated_at
		, USERD.login as loginD, USR.deleted_at
		FROM ` + tblPrefix + `USR USR 
		left join  ` + tblPrefix + `USR USERC on USERC.id = USR.created_by
		left join  ` + tblPrefix + `USR USERU on USERU.id = USR.updated_by
		left join  ` + tblPrefix + `USR USERD on USERD.id = USR.deleted_by
		` + filter.GetSQLWhere()
	q = filter.AppendPaging(q, nbRow.Int64)

	rows, err := MainDB.Query(q, filter.SQLParams...)
	if err != nil {
		return nil, pagedResp, fmt.Errorf("UserList query %w", err)
	}
	defer rows.Close()
	var (
		id         int
		name       sql.NullString
		login      sql.NullString
		pwd        sql.NullString
		loginC     sql.NullString
		loginU     sql.NullString
		loginD     sql.NullString
		rightlevel sql.NullInt64
		createdAt  sql.NullTime
		updatedAt  sql.NullTime
		deletedAt  sql.NullTime
	)
	for rows.Next() {
		err = rows.Scan(&id, &name, &login, &pwd, &rightlevel, &loginC, &createdAt, &loginU, &updatedAt, &loginD, &deletedAt)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("UserList scan %w", err)
		}
		arr = append(arr, DbUser{
			ID:           id,
			Name:         name.String,
			Login:        login.String,
			PasswordHash: pwd.String,
			RightLevel:   int(rightlevel.Int64),
			Deleted:      deletedAt.Valid,
			Info:         stdInfo(&loginC, &loginU, &loginD, &createdAt, &updatedAt, &deletedAt),
		})
	}
	if rows.Err() != nil && rows.Err() != sql.ErrNoRows {
		return nil, pagedResp, fmt.Errorf("UserList err %w", err)
	}
	pagedResp.Data = arr

	return arr, pagedResp, nil
}

// UserLoginAvailable retourne vrai si le login est dispo
func UserLoginAvailable(login string) bool {
	q := ` SELECT 1 FROM ` + tblPrefix + `USR USR where USR.login = ? `
	err := MainDB.QueryRow(q, login).Scan()
	return err == sql.ErrNoRows
}

// UserGet get d'un user
func UserGet(id int) (DbUser, error) {
	var ret DbUser
	filter := NewSearchQueryFromID("USR", id)

	arr, _, err := UserList(filter)
	if err != nil {
		return ret, err
	}
	if len(arr) > 0 {
		ret = arr[0]
	}
	return ret, nil
}

// UserCheckAuth authentification user
func UserCheckAuth(login string, password string) (DbUser, error) {
	credErr := fmt.Errorf("invalid user/password")

	//interro user de par le login
	sq := SearchQuery{
		Offset:    0,
		Limit:     1,
		SQLFilter: "USR.login = ?",
		SQLParams: []interface{}{login},
	}
	arr, _, err := UserList(sq)

	if err != nil {
		return DbUser{}, err
	}
	if len(arr) != 1 {
		return DbUser{}, credErr
	}

	//ctrl password
	err = bcrypt.CompareHashAndPassword([]byte(arr[0].PasswordHash), []byte(password))
	if err != nil {
		return DbUser{}, credErr
	}

	return arr[0], nil
}

// UserUpdate maj user
func UserUpdate(elm DbUser, usrUpdater int, tx *sql.Tx) error {
	strDelQ := ""
	if !elm.Deleted {
		strDelQ = ", deleted_by = NULL, deleted_at = NULL"
	} else {
		strDelQ = ", deleted_by = " + strconv.Itoa(usrUpdater) + ", deleted_at = '" + time.Now().Format("2006-01-02T15:04:05.999") + "'"
	}

	q := `UPDATE ` + tblPrefix + `USR SET
		updated_by = ?, updated_at = ? ` + strDelQ + `
		, name = ?, login = ?, rightlevel = ?
		where id = ? `
	_, err := TxExec(tx, q, usrUpdater, time.Now(), elm.Name, elm.Login, elm.RightLevel, elm.ID)
	if err != nil {
		return fmt.Errorf("UserUpdate err %w", err)
	}

	//maj password
	if elm.Password != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(elm.Password), bcrypt.DefaultCost) //bcrypt inclus d??ja un salt
		if err != nil {
			return fmt.Errorf("bcrypt err %w", err)
		}

		_, err = TxExec(tx, `UPDATE `+tblPrefix+`USR SET password = ? where id = ? `, string(hash), elm.ID)
		if err != nil {
			return fmt.Errorf("UserUpdate err %w", err)
		}
	}
	return nil
}

// UserDelete flag user suppression
func UserDelete(elmID int, usrUpdater int) error {
	q := `UPDATE ` + tblPrefix + `USR SET deleted_by = ?, deleted_at = ? where id = ? `
	_, err := TxExec(nil, q, usrUpdater, time.Now(), elmID)
	if err != nil {
		return fmt.Errorf("UserDelete err %w", err)
	}
	return nil
}

// UserInsert flag user suppression
func UserInsert(elm *DbUser, usrUpdater int) error {
	tx, err := MainDB.Begin()
	if err != nil {
		return fmt.Errorf("UserInsert err %w", err)
	}
	defer tx.Rollback()

	//insert base
	q := `INSERT INTO ` + tblPrefix + `USR (login, created_by, created_at) VALUES(?,?,?) `
	id, err := TxInsert(tx, q, elm.Login, usrUpdater, time.Now())
	if err != nil {
		return fmt.Errorf("UserInsert err %w", err)
	}

	//mj pour le reste des champs
	elm.ID = int(id)
	err = UserUpdate(*elm, usrUpdater, tx)
	if err != nil {
		return fmt.Errorf("UserInsert err %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("UserInsert err %w", err)
	}
	return nil
}
