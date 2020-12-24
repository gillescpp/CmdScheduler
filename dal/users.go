package dal

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// UserList liste des users
func UserList(filter SearchQuery) ([]DbUser, error) {
	var err error
	arr := make([]DbUser, 0)

	//nb rows
	var nbRow sql.NullInt64
	if filter.Limit > 1 {
		q := ` SELECT count(*) as Nb FROM FROM ` + tblPrefix + `USER where ` + filter.SQLFilter
		err = MainDB.QueryRow(q, filter.SQLParams...).Scan(&nbRow)
		if err != nil {
			return nil, fmt.Errorf("UserList NbRow %w", err)
		}
	}

	// listing
	q := ` SELECT id, name, login, deleted_at
		FROM ` + tblPrefix + `USER where ` + filter.SQLFilter
	q = filter.AppendPaging(q, nbRow.Int64)

	rows, err := MainDB.Query(q, filter.SQLParams...)
	if err != nil {
		return nil, fmt.Errorf("UserList query %w", err)
	}
	defer rows.Close()
	var (
		id        int
		name      sql.NullString
		login     sql.NullString
		deletedAt sql.NullTime
	)
	for rows.Next() {
		err = rows.Scan(&id, &name, &login, &deletedAt)
		if err != nil {
			return nil, fmt.Errorf("UserList scan %w", err)
		}
		arr = append(arr, DbUser{
			ID:    id,
			Name:  name.String,
			Login: login.String,
			Activ: !deletedAt.Valid,
		})
	}
	if rows.Err() != nil && rows.Err() != sql.ErrNoRows {
		return nil, fmt.Errorf("UserList err %w", err)
	}

	return arr, nil
}

// UserGet get d'un user
func UserGet(id int) (DbUser, error) {
	var ret DbUser
	filter := NewSearchQueryFromID(id)

	arr, err := UserList(filter)
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
	if err != sql.ErrNoRows || usrID.Int64 == 0 {
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
func UserUpdate(elm DbUser, usrUpdater int, admin bool) error {
	strDelQ := ""
	if admin {
		if elm.Activ {
			strDelQ = ", deleted_by = NULL, deleted_at = NULL"
		} else {
			strDelQ = ", deleted_by = " + strconv.Itoa(usrUpdater) + ", deleted_at = '" + time.Now().Format("2006-01-02T15:04:05.999") + "'"
		}
	}

	q := `UPDATE ` + tblPrefix + `USER SET
		updated_by = ?, updated_at = ? ` + strDelQ + `
		, name = ?, login = ?
		where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elm.Name, elm.Login, elm.ID)
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
	q := `INSERT INTO ` + tblPrefix + `USER (created_by, created_at) VALUES(?,?) `
	res, err := MainDB.Exec(q, usrUpdater, time.Now())
	if err != nil {
		return fmt.Errorf("UserInsert err %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("UserInsert err %w", err)
	}

	//mj pour le reste des champs
	elm.ID = int(id)
	UserUpdate(*elm, usrUpdater, true)

	_, err = MainDB.Exec(`COMMIT TRANSACTION`)
	if err != nil {
		return fmt.Errorf("UserInsert err %w", err)
	}
	return nil
}
