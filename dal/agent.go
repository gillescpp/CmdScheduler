package dal

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Agent : Agents d'execution des taches

// AgentList liste des users
func AgentList(filter SearchQuery) ([]DbAgent, PagedResponse, error) {
	var err error
	arr := make([]DbAgent, 0)
	var pagedResp PagedResponse

	//nb rows
	var nbRow sql.NullInt64
	if filter.Limit > 1 {
		q := ` SELECT count(*) as Nb FROM ` + tblPrefix + `AGENT AGENT ` + filter.GetSQLWhere()
		err = MainDB.QueryRow(q, filter.SQLParams...).Scan(&nbRow)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("AgentList NbRow %w", err)
		}
	}

	//pour retour d'info avec info paging
	pagedResp = NewPagedResponse(arr, filter, int(nbRow.Int64))

	// listing
	q := ` SELECT AGENT.id, AGENT.host, AGENT.apikey, AGENT.certsignallowed
		, USERC.login as loginC, AGENT.created_at
		, USERU.login as loginU, AGENT.updated_at
		, USERD.login as loginD, AGENT.deleted_at
		FROM ` + tblPrefix + `AGENT AGENT 
		left join  ` + tblPrefix + `USER USERC on USERC.id = AGENT.created_by
		left join  ` + tblPrefix + `USER USERU on USERU.id = AGENT.updated_by
		left join  ` + tblPrefix + `USER USERD on USERD.id = AGENT.deleted_by
		` + filter.GetSQLWhere()
	q = filter.AppendPaging(q, nbRow.Int64)

	rows, err := MainDB.Query(q, filter.SQLParams...)
	if err != nil {
		return nil, pagedResp, fmt.Errorf("AgentList query %w", err)
	}
	defer rows.Close()
	var (
		id        int
		host      sql.NullString
		apikey    sql.NullString
		certsign  sql.NullString
		loginC    sql.NullString
		loginU    sql.NullString
		loginD    sql.NullString
		createdAt sql.NullTime
		updatedAt sql.NullTime
		deletedAt sql.NullTime
	)
	for rows.Next() {
		err = rows.Scan(&id, &host, &apikey, &certsign, &loginC, &createdAt, &loginU, &updatedAt, &loginD, &deletedAt)
		if err != nil {
			return nil, pagedResp, fmt.Errorf("AgentList scan %w", err)
		}
		arr = append(arr, DbAgent{
			ID:              id,
			Host:            host.String,
			APIKey:          apikey.String,
			CertSignAllowed: certsign.String,
			Tls:             strings.HasPrefix(host.String, "https://"),
			Deleted:         deletedAt.Valid,
			Info:            stdInfo(&loginC, &loginU, &loginD, &createdAt, &updatedAt, &deletedAt),
		})
	}
	if rows.Err() != nil && rows.Err() != sql.ErrNoRows {
		return nil, pagedResp, fmt.Errorf("AgentList err %w", err)
	}
	pagedResp.Data = arr

	return arr, pagedResp, nil
}

// AgentHostNotExists retourne vrai si le host n'existe pas déja
func AgentHostNotExists(host string) bool {
	q := ` SELECT 1 FROM ` + tblPrefix + `AGENT AGENT where AGENT.host = ? `
	err := MainDB.QueryRow(q, host).Scan()
	return err == sql.ErrNoRows
}

// AgentGet get d'un user
func AgentGet(id int) (DbAgent, error) {
	var ret DbAgent
	filter := NewSearchQueryFromID("AGENT", id)

	arr, _, err := AgentList(filter)
	if err != nil {
		return ret, err
	}
	if len(arr) > 0 {
		ret = arr[0]
	}
	return ret, nil
}

// AgentUpdate maj user
func AgentUpdate(elm DbAgent, usrUpdater int) error {
	strDelQ := ""
	if !elm.Deleted {
		strDelQ = ", deleted_by = NULL, deleted_at = NULL"
	} else {
		strDelQ = ", deleted_by = " + strconv.Itoa(usrUpdater) + ", deleted_at = '" + time.Now().Format("2006-01-02T15:04:05.999") + "'"
	}

	q := `UPDATE ` + tblPrefix + `AGENT SET
		updated_by = ?, updated_at = ?  ` + strDelQ + `
		, host = ?, apikey = ?, certsignallowed = ?
		where id = ? `

	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elm.Host, elm.APIKey, elm.CertSignAllowed, elm.ID)
	if err != nil {
		return fmt.Errorf("AgentUpdate err %w", err)
	}

	return nil
}

// AgentDelete flag agent suppression
func AgentDelete(elmID int, usrUpdater int) error {
	q := `UPDATE ` + tblPrefix + `AGENT SET deleted_by = ?, deleted_at = ? where id = ? `
	_, err := MainDB.Exec(q, usrUpdater, time.Now(), elmID)
	if err != nil {
		return fmt.Errorf("AgentDelete err %w", err)
	}
	return nil
}

// AgentInsert insertion agent
func AgentInsert(elm *DbAgent, usrUpdater int) error {
	_, err := MainDB.Exec(`BEGIN TRANSACTION`)
	if err != nil {
		return fmt.Errorf("AgentInsert err %w", err)
	}
	defer func() {
		MainDB.Exec(`ROLLBACK TRANSACTION`)
	}()

	//insert base
	q := `INSERT INTO ` + tblPrefix + `AGENT (created_by, created_at) VALUES(?,?) `
	res, err := MainDB.Exec(q, usrUpdater, time.Now())
	if err != nil {
		return fmt.Errorf("AgentInsert err %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("AgentInsert err %w", err)
	}

	//mj pour le reste des champs
	elm.ID = int(id)
	err = AgentUpdate(*elm, usrUpdater)
	if err != nil {
		return fmt.Errorf("AgentInsert err %w", err)
	}

	_, err = MainDB.Exec(`COMMIT TRANSACTION`)
	if err != nil {
		return fmt.Errorf("AgentInsert err %w", err)
	}
	return nil
}
