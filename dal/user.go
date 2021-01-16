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
		q := ` SELECT count(*) as Nb FROM ` + tblPrefix + `USER ` + filter.GetSQLWhere()
		err = MainDB.QueryRow(q, filter.SQLParams...).Scan(&nbRow)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("UserList NbRow %w", err)
		}
	}

	//pour retour d'info avec info paging
	pagedResp = NewPagedResponse(arr, filter.Offset, filter.Limit, int(nbRow.Int64))

	// listing
	q := ` SELECT id, name, login, rightlevel, deleted_at
		FROM ` + tblPrefix + `USER ` + filter.GetSQLWhere()
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
		rightlevel sql.NullInt64
		deletedAt  sql.NullTime
	)
	for rows.Next() {
		err = rows.Scan(&id, &name, &login, &rightlevel, &deletedAt)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("UserList scan %w", err)
		}
		arr = append(arr, DbUser{
			ID:         id,
			Name:       name.String,
			Login:      login.String,
			RightLevel: int(rightlevel.Int64),
			Deleted:    deletedAt.Valid,
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
	q := ` SELECT 1 FROM ` + tblPrefix + `USER where login = ? `
	err := MainDB.QueryRow(q, login).Scan()
	if err == sql.ErrNoRows {
		return true
	}
	return false
}

// UserGet get d'un user
func UserGet(id int) (DbUser, error) {
	var ret DbUser
	filter := NewSearchQueryFromID(id)

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
func UserCheckAuth(login string, password string) (int, error) {
	var err error
	var usrID sql.NullInt64
	var usrPassword sql.NullString

	q := ` SELECT id, password FROM ` + tblPrefix + `USER where deleted_at is null and login = ? `
	err = MainDB.QueryRow(q, login).Scan(&usrID, &usrPassword)
	if err == sql.ErrNoRows || usrID.Int64 == 0 {
		return 0, fmt.Errorf("Invalid user.password")
	} else if err != nil {
		return 0, fmt.Errorf("UserCheckAuth %w", err)
	}

	//ctrl password
	err = bcrypt.CompareHashAndPassword([]byte(usrPassword.String), []byte(password))
	if err != nil {
		return 0, fmt.Errorf("Invalid user.password")
	}

	return int(usrID.Int64), nil
}

// UserUpdate maj user
func UserUpdate(elm DbUser, usrUpdater int) error {
	strDelQ := ""
	if !elm.Deleted {
		strDelQ = ", deleted_by = NULL, deleted_at = NULL"
	} else {
		strDelQ = ", deleted_by = " + strconv.Itoa(usrUpdater) + ", deleted_at = '" + time.Now().Format("2006-01-02T15:04:05.999") + "'"
	}

	q := `UPDATE ` + tblPrefix + `USER SET
		updated_by = ?, updated_at = ? ` + strDelQ + `
		, name = ?
		where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elm.Name, elm.ID)
	if err != nil {
		return fmt.Errorf("UserUpdate err %w", err)
	}

	//maj password
	if elm.Password != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(elm.Password), bcrypt.DefaultCost) //bcrypt inclus déja un salt
		if err != nil {
			return fmt.Errorf("bcrypt err %w", err)
		}

		_, err = MainDB.Exec(`UPDATE `+tblPrefix+`USER SET password = ? where id = ? `, string(hash), elm.ID)
		if err != nil {
			return fmt.Errorf("UserUpdate err %w", err)
		}
	}
	return nil
}

// UserDelete flag user suppression
func UserDelete(elmID int, usrUpdater int) error {
	q := `UPDATE ` + tblPrefix + `USER SET deleted_by = ?, deleted_at = ? where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elmID)
	if err != nil {
		return fmt.Errorf("UserDelete err %w", err)
	}
	return nil
}

// UserInsert flag user suppression
func UserInsert(elm *DbUser, usrUpdater int) error {
	_, err := MainDB.Exec(`BEGIN TRANSACTION`)
	if err != nil {
		return fmt.Errorf("UserInsert err %w", err)
	}
	defer func() {
		MainDB.Exec(`ROLLBACK TRANSACTION`)
	}()

	//insert base
	q := `INSERT INTO ` + tblPrefix + `USER (login, created_by, created_at) VALUES(?, ?,?) `
	res, err := MainDB.Exec(q, elm.Login, usrUpdater, time.Now())
	if err != nil {
		return fmt.Errorf("UserInsert err %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("UserInsert err %w", err)
	}

	//mj pour le reste des champs
	elm.ID = int(id)
	err = UserUpdate(*elm, usrUpdater)
	if err != nil {
		return fmt.Errorf("UserInsert err %w", err)
	}

	_, err = MainDB.Exec(`COMMIT TRANSACTION`)
	if err != nil {
		return fmt.Errorf("UserInsert err %w", err)
	}
	return nil
}
