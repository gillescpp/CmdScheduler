package dal

import (
	"database/sql"
	"strconv"
	"strings"
)

// RightLevel represente un niveau de droit
// (pas de gestion granularité un niveau n a toute les droits <=n aquis )
type RightLevel int

// Constante palier de droit actuellement en place
const (
	RightLvlAdmin       = 100 // Droit niveau Admin
	RightLvlTaskBuilder = 50  // Droit niveau configurateur de tache
	RightLvlTaskRunner  = 10  // Droit niveau lanceur/planif de tache
	RightLvlViewer      = 1   // Droit niveau viewer
)

// IsAutorised retourne si le droit est applicable au righet level
// paliers défini en dur dans cette fonction
// 100 : admin
// 50 : Task Builder
// 10 : Task Launcher / Planif
// 1 : Viewer
func IsAutorised(rightlevel RightLevel, crudcode string, edit bool) bool {
	allowed := false
	crudcode = strings.ToUpper(crudcode)
	switch {
	case (crudcode == "USER"):
		allowed = (rightlevel >= RightLvlAdmin)
	case (crudcode == "AGENT"):
		allowed = (!edit && (rightlevel >= RightLvlTaskBuilder)) || (edit && (rightlevel >= RightLvlAdmin))
	case (crudcode == "TASK"):
		allowed = (!edit && (rightlevel >= RightLvlTaskRunner)) || (edit && (rightlevel >= RightLvlTaskBuilder))
	case (crudcode == "QUEUE"):
		allowed = (!edit && (rightlevel >= RightLvlViewer)) || (edit && (rightlevel >= RightLvlAdmin))
	case (crudcode == "TAGS"):
		allowed = (!edit && (rightlevel >= RightLvlViewer)) || (edit && (rightlevel >= RightLvlAdmin))
	case (crudcode == "CONFIG"):
		allowed = (!edit && (rightlevel >= RightLvlAdmin)) || (edit && (rightlevel >= RightLvlAdmin))
	case (crudcode == "TASKFLOW"):
		allowed = (!edit && (rightlevel >= RightLvlTaskRunner)) || (edit && (rightlevel >= RightLvlTaskBuilder))
	case (crudcode == "SCHED"):
		allowed = (!edit && (rightlevel >= RightLvlViewer)) || (edit && (rightlevel >= RightLvlTaskRunner))
	}
	return allowed
}

// Filter config filtre pour listing
/*
type Filter struct {
	ShowDeleted    bool //voir les records marqués deleted
	OtherSQLFilter string
	OnlyThisID     int //mode 1 id
}

// ToSQL retourne le filtre sql
func (c Filter) ToSQL(alias string, noEmpty bool) (string, []interface{}) {
	arrP := make([]interface{}, 0)
	sqlReturn := ""
	sqlAlias := ""
	if alias != "" {
		sqlAlias = "." + alias
	}

	if c.OnlyThisID > 0 {
		sqlReturn += sqlAlias + "ID = ?"
		arrP = append(arrP, &c.OnlyThisID)
		return sqlReturn, arrP
	}
	if !c.ShowDeleted {
		sqlReturn += sqlAlias + "deleted_at is null and"
	}
	if c.OtherSQLFilter != "" {
		sqlReturn += c.OtherSQLFilter + " and"
	}

	if sqlReturn != "" { //suppression dernier and
		sqlReturn = sqlReturn[0 : len(sqlReturn)-4]
	}
	if noEmpty && sqlReturn == "" {
		sqlReturn = "1=1"
	}
	return sqlReturn, arrP
}
*/

// Paging config paging pour listing
type Paging struct {
	Page       int    //n° de page
	Sort       string //champs de tri
	RowPerPage int    //nombre de libre par page
}

// AppendPaging Util gestion du paging
func (c Paging) AppendPaging(sql string, rowcount int64) string {
	// calcul offset
	sqlReturn := sql
	offset := 0
	if rowcount <= 0 || c.RowPerPage <= 0 {
		//pas de paging sans nombre de page, ou row/page
		return sqlReturn
	}
	if c.Page > 0 && rowcount > 0 {
		offset = (c.Page - 1) * c.RowPerPage
	}

	if strings.EqualFold(dbDriver, "mssql") {
		// select * from <tabl>
		// order by <>			tri imposé si utilisation de FETCH (il faut utiliser seulement OFFSET avec un TOP pour la limit dans ce cas)
		// OFFSET 1 ROWS 		offset (0=a partir du premier, 1, du deuxieme)
		// FETCH NEXT 3 ROWS ONLY -- row par page, order by obligatoire pour cett instruction
		if c.Sort != "" {
			sqlAppend := " order by " + c.Sort +
				" OFFSET " + strconv.Itoa(offset) + " ROWS " +
				" FETCH NEXT " + strconv.Itoa(c.RowPerPage) + " ROWS ONLY "

			sqlReturn = sql + sqlAppend
		} else {
			sqlReturn = " select TOP " + strconv.Itoa(c.RowPerPage) + " * FROM (" + sql + ") T OFFSET " + strconv.Itoa(offset) + " ROWS "
		}
	} else {
		// order by Sort LIMIT offset, row_count;
		sqlAppend := ""
		if c.Sort != "" {
			sqlAppend += " order by " + c.Sort
		}
		sqlAppend += " LIMIT " + strconv.Itoa(offset) + ", " + strconv.Itoa(c.RowPerPage)

		sqlReturn = sql + sqlAppend
	}
	return sqlReturn
}

// CfgKVGet getter table de config kv
func CfgKVGet(key string) (string, error) {
	var err error
	key = strings.ToLower(strings.TrimSpace(key))

	var val sql.NullString
	err = MainDB.QueryRow("SELECT KVAL FROM "+tblPrefix+"CFG where KID = ?", key).Scan(&val)
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}

	return val.String, nil
}

// CfgKVSet setter table de config kv
func CfgKVSet(key string, val string) error {
	var err error
	key = strings.ToLower(strings.TrimSpace(key))

	var n sql.NullInt64
	err = MainDB.QueryRow("SELECT 1 FROM "+tblPrefix+"CFG where KID = ?", key).Scan(&n)
	if err == sql.ErrNoRows {
		_, err = MainDB.Exec(`INSERT INTO `+tblPrefix+`CFG (KID, KVAL) VALUES(?, NULL) `, key)
	}
	if err != nil {
		return err
	}

	if val == "" {
		_, err = MainDB.Exec(`DELETE FROM `+tblPrefix+`CFG WHERE KID = ?`, key)
	} else {
		_, err = MainDB.Exec(`UPDATE `+tblPrefix+`CFG SET KVAL = ? WHERE KID = ?`, val, key)
	}
	if err != nil {
		return err
	}
	return nil
}

// CfgKVIsSystem retourne vrai pour les clés de config system
func CfgKVIsSystem(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	if strings.HasPrefix(key, "web.") || strings.HasPrefix(key, "db.") {
		return true
	}
	return false
}

// KVJSON réprésentation d'un kv
type KVJSON struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// CfgKVList liste kv
func CfgKVList() ([]KVJSON, error) {
	arr := make([]KVJSON, 0)

	var k, v sql.NullString
	rows, err := MainDB.Query("SELECT KID, KVAL FROM " + tblPrefix + "CFG ORDER BY KID")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&k, &v)
		if err != nil {
			return nil, err
		}
		arr = append(arr, KVJSON{
			Key:   k.String,
			Value: v.String,
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return arr, nil
}
