package dal

import (
	"database/sql"
	"fmt"
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
		q := ` SELECT count(*) as Nb FROM ` + tblPrefix + `PERIOD PERIOD ` + filter.GetSQLWhere()
		err = MainDB.QueryRow(q, filter.SQLParams...).Scan(&nbRow)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("SchedList NbRow %w", err)
		}
	}

	//pour retour d'info avec info paging
	pagedResp = NewPagedResponse(arr, filter, int(nbRow.Int64))

	// listing
	q := ` SELECT PERIOD.id, PERIOD.lib, PERIOD.type, PERIOD.time_zone 
		, USERC.login as loginC, PERIOD.created_at
		, USERU.login as loginU, PERIOD.updated_at
		FROM ` + tblPrefix + `PERIOD PERIOD 
		left join  ` + tblPrefix + `USER USERC on USERC.id = PERIOD.created_by
		left join  ` + tblPrefix + `USER USERU on USERU.id = PERIOD.updated_by
		` + filter.GetSQLWhere()
	q = filter.AppendPaging(q, nbRow.Int64)

	rows, err := MainDB.Query(q, filter.SQLParams...)
	if err != nil {
		return nil, pagedResp, fmt.Errorf("SchedList query %w", err)
	}
	defer rows.Close()
	var (
		id        int
		lib       sql.NullString
		typep     sql.NullInt64
		timeZone  sql.NullString
		createdAt sql.NullTime
		updatedAt sql.NullTime
		loginC    sql.NullString
		loginU    sql.NullString
	)
	for rows.Next() {
		err = rows.Scan(&id, &lib, &typep, &timeZone,
			&loginC, &createdAt, &loginU, &updatedAt)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("SchedList scan %w", err)
		}
		zn, _ := time.LoadLocation(timeZone.String)
		arr = append(arr, DbSched{
			ID:       id,
			Lib:      lib.String,
			IsPeriod: (typep.Int64 == 0),
			TimeZone: timeZone.String,
			zone:     zn,
			Detail:   []DbSchedDetail{},
			Info:     stdInfo(&loginC, &loginU, nil, &createdAt, &updatedAt, nil),
		})
		arrMp[id] = len(arr) - 1
	}
	if rows.Err() != nil && rows.Err() != sql.ErrNoRows {
		return nil, pagedResp, fmt.Errorf("SchedList err %w", err)
	}

	//detail
	if len(arr) > 0 {
		idarr := make([]interface{}, len(arr))
		q = ` SELECT PERIODDETAIL.periodid, PERIODDETAIL.interval, PERIODDETAIL.intervalhours, PERIODDETAIL.hours, 
			PERIODDETAIL.months, PERIODDETAIL.weekdays, PERIODDETAIL.monthdays
			FROM ` + tblPrefix + `PERIODDETAIL PERIODDETAIL where PERIODDETAIL.periodid in (0`
		for i := 0; i < len(arr); i++ {
			q += `,?`
			idarr[i] = arr[i].ID
		}
		q += `) order by PERIODDETAIL.periodid, PERIODDETAIL.idx`

		rowsDet, err := MainDB.Query(q, idarr...)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("SchedList det query %w", err)
		}
		defer rowsDet.Close()
		var (
			periodid      int
			interval      sql.NullInt64
			intervalhours sql.NullString
			hours         sql.NullString
			months        sql.NullString
			weekdays      sql.NullString
			monthdays     sql.NullString
		)
		for rowsDet.Next() {
			err = rowsDet.Scan(&periodid, &interval, &intervalhours,
				&hours, &months, &weekdays, &monthdays)
			if err != nil {
				return nil, pagedResp, fmt.Errorf("SchedList det scan %w", err)
			}
			arr[arrMp[periodid]].Detail = append(arr[arrMp[periodid]].Detail, DbSchedDetail{
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
	filter := NewSearchQueryFromID("PERIOD", id)

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
	if transaction {
		_, err := MainDB.Exec(`BEGIN TRANSACTION`)
		if err != nil {
			return fmt.Errorf("SchedUpdate err %w", err)
		}
		defer func() {
			MainDB.Exec(`ROLLBACK TRANSACTION`)
		}()
	}

	typep := 0
	if !elm.IsPeriod {
		typep = 1
	}
	q := `UPDATE ` + tblPrefix + `PERIOD SET
		updated_by = ?, updated_at = ?, lib = ?, type = ?, time_zone = ?
		where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elm.Lib, typep, elm.TimeZone, elm.ID)
	if err != nil {
		return fmt.Errorf("SchedUpdate err %w", err)
	}

	//detail par delete/insert
	q = `DELETE FROM ` + tblPrefix + `PERIODDETAIL where periodid = ? `
	_, err = MainDB.Exec(q, elm.ID)
	if err != nil {
		return fmt.Errorf("SchedUpdate err %w", err)
	}

	q = `INSERT INTO ` + tblPrefix + `PERIODDETAIL(periodid, idx, interval, intervalhours
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
	_, err := MainDB.Exec(`BEGIN TRANSACTION`)
	if err != nil {
		return fmt.Errorf("SchedDelete err %w", err)
	}
	defer func() {
		MainDB.Exec(`ROLLBACK TRANSACTION`)
	}()

	q := `DELETE FROM ` + tblPrefix + `PERIODDETAIL where periodid = ? `
	_, err = MainDB.Exec(q, elmID)
	if err != nil {
		return fmt.Errorf("SchedDelete err %w", err)
	}

	q = `DELETE FROM ` + tblPrefix + `PERIOD where id = ? `
	_, err = MainDB.Exec(q, elmID)
	if err != nil {
		return fmt.Errorf("SchedDelete err %w", err)
	}

	_, err = MainDB.Exec(`COMMIT TRANSACTION`)
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
	q := `INSERT INTO ` + tblPrefix + `PERIOD (created_by, created_at) VALUES(?,?) `
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
