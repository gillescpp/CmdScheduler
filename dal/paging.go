package dal

import (
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Helpers paging

//variable globale controleur
var (
	DefaultRecordPerPage = 25
	MaxRecordPerPage     = 100
)

// PagedResponse est un container pour les réponse avec pagin
type PagedResponse struct {
	Offset      int `json:"offset"`              // Offset courant
	Limit       int `json:"limit,omitempty"`     // record par page
	TotalRecord int `json:"totalRecord"`         // nombre de record total
	Page        int `json:"page,omitempty"`      // Page courant
	TotalPage   int `json:"totalPage,omitempty"` // nombre de page

	Data interface{} `json:"data"` //(table de) résultat
}

// NewPagedResponse init d'un struct PagedResponse
func NewPagedResponse(data interface{}, sq SearchQuery, recordCount int) PagedResponse {
	var pr PagedResponse
	pr.Offset = sq.Offset
	pr.Limit = sq.Limit
	pr.TotalRecord = recordCount
	pr.Data = data
	if pr.Offset < 0 {
		pr.Offset = 0
	}
	if pr.Limit <= 0 {
		pr.Limit = 0
		pr.Page = 1
		pr.TotalPage = 1
	}
	if pr.TotalRecord < 0 {
		pr.TotalRecord = 0
	}
	//page offset 0-9, page 1, limit 10
	if pr.Limit > 0 {
		pr.Page = 1
		pr.TotalPage = 1
		if pr.TotalRecord > pr.Limit {
			pr.TotalPage = ((pr.TotalRecord - 1) / pr.Limit) + 1
			pr.Page = (pr.Offset / pr.Limit) + 1
		}
	}

	return pr
}

// SearchQuery represente les elements de filtre, paging et tri d'une requete
type SearchQuery struct {
	Offset  int    // (input) Offset à appliquer
	Limit   int    // (input) nb record max
	SQLSort string // (input) requete partie order by

	SQLFilter string        //(calculé par NewSearchQueryFromXX) filtre sql
	SQLParams []interface{} //(calculé par NewSearchQueryFromXX) champ sort
}

// GetSQLWhere Util where sql
func (c SearchQuery) GetSQLWhere() string {
	if c.SQLFilter == "" {
		return ""
	}
	return " where " + c.SQLFilter
}

// AppendPaging Util gestion du paging
func (c SearchQuery) AppendPaging(sql string, rowcount int64) string {
	// calcul offset
	sqlReturn := sql
	if rowcount <= 0 || c.Limit <= 0 {
		//pas de paging sans nombre de page, ou row/page
		return sqlReturn
	}

	if strings.EqualFold(dbDriver, "mssql") {
		// select * from <tabl>
		// tri imposé si utilisation de OFFSET
		// OFFSET 1 ROWS 		offset (0=a partir du premier, 1, du deuxieme)
		// FETCH NEXT 3 ROWS ONLY -- row par page, order by obligatoire pour cett instruction
		if c.SQLSort != "" {
			sqlAppend := " order by " + c.SQLSort +
				" OFFSET " + strconv.Itoa(c.Offset) + " ROWS " +
				" FETCH NEXT " + strconv.Itoa(c.Limit) + " ROWS ONLY "

			sqlReturn = sql + sqlAppend
		}
	} else {
		// order by Sort LIMIT offset, row_count;
		sqlAppend := ""
		if c.SQLSort != "" {
			sqlAppend += " order by " + c.SQLSort
		}
		sqlAppend += " LIMIT " + strconv.Itoa(c.Offset) + ", " + strconv.Itoa(c.Limit)

		sqlReturn = sql + sqlAppend
	}
	return sqlReturn
}

//NewSearchQueryFromID filtre id unique
func NewSearchQueryFromID(prefix string, id int) SearchQuery {
	sq := SearchQuery{
		Offset:    0,
		Limit:     1,
		SQLFilter: prefix + ".id = ?",
		SQLParams: []interface{}{id},
	}
	return sq
}

//NewSearchQueryFromRequest recup filtre dans le GET
//param pour le paging :
// * offset : offset query
// * page : alternative à l'offset (un offset est aors calculé sur la base de cette valeur)
// * limit : nb rec max
// * sort : champs de tri (ex: sort=name, sort=name,age, sort=name,desc:age)
// Tout autres parametre est utilisé pour le filtrage (pas de requete complexe possible conbinant AND/OR de maniere groupé)
//  item_name=test			*
//  item_name=eq:test		* operateur : eq, not, like, gt (>), gte (>=), lt (<), lte (<=), in (suivi liste sep par ,)
//  item_name=gt:5.00 & item_name=lte:25.00				* AND appliqué sur un même param
//  item_name=test & price=gt:5.00						* AND sur deux param
//  + op "oeq" ("or equal")
// structInfo est un ptr vers une struct de type "entité". La struct doit être annoté
// afin de fournir les capacités offertes sur chaque champs : recherchable, et/ou utilisable dans les tries
// annotation attendu :
//  apiuse:"search,sort" : indique le champs concerné est utilisable dans les filtres et/ou en tant qu'element de trie
//                         Si le champs n'a aucune capacité, il sera ignoré si présent dans les filtres
//  dbname:"XXX" : (optionel) Eventuel nom du champs sur la bdd si différent du nom du champs
//  apiname:"XXX" : (optionel) Eventuel nom du champs utilisé coté client si différent du nom du champs
//  dbtype:"XXX" : (optionel) Eventuel type du champs sur la bdd si différent du nom du champs (int, float, date, time, datetime, string)
func NewSearchQueryFromRequest(r *http.Request, structInfo interface{}, FromForm bool) SearchQuery {
	//référencement des champs à capacité
	dbfields := extractSearchInfo(structInfo)

	//extraction param de la requete
	var urlVals url.Values
	if FromForm {
		//depuis body ou url
		err := r.ParseForm()
		if err != nil {
			urlVals = r.Form
		}
	} else {
		//depuis params GET seulement
		urlVals = r.URL.Query()
	}

	//rien a faire
	if len(urlVals) == 0 {
		//tri par defaut
		sort := ""
		for _, f := range dbfields {
			if f.Sortable && strings.HasSuffix(f.DbName, ".id") {
				sort = f.DbName + " ASC"
				break
			}
		}
		return SearchQuery{
			Limit:   DefaultRecordPerPage,
			SQLSort: sort,
		}
	}

	var (
		pParamsVals       = make([]interface{}, 0)
		qOffset           = 0
		qPage             = 0
		qLimit            = DefaultRecordPerPage
		qSortFieldE       = make(map[string]bool)
		qSortFieldO       = make([]string, 0)
		pQueryFieldFilter = make([]string, 0)
	)

	// parcours des parametres de la requete
	for pname, vals := range urlVals {
		// k est le nom du param, vals est la/les valeurs assignés (le même param peut être specifié plusieurs fois)
		pname = strings.TrimSpace(strings.ToLower(pname))
		if pname == "" {
			continue
		}
		// champs spéciaux : offset, page, limit, sort
		if pname == "offset" {
			if len(vals) >= 1 {
				qOffset, _ = strconv.Atoi(vals[0])
			}
		} else if pname == "page" {
			if len(vals) >= 1 {
				qPage, _ = strconv.Atoi(vals[0])
			}
		} else if pname == "limit" {
			if len(vals) >= 1 {
				qLimit, _ = strconv.Atoi(vals[0])
			}
		} else if pname == "sort" {
			// un champ a sep virgule ou plusieurs champs
			var arr []string
			if len(vals) == 1 {
				arr = strings.Split(vals[0], ",")
			} else {
				arr = vals
			}
			for _, s := range arr {
				s = strings.TrimSpace(strings.ToLower(s))
				sortdesc := false
				if strings.HasPrefix(s, "asc:") {
					s = strings.TrimPrefix(s, "asc:")
				}
				if strings.HasPrefix(s, "desc:") {
					s = strings.TrimPrefix(s, "desc:")
					sortdesc = true
				}
				// le champs spécifié doit être triable
				if fieldMeta, exists := dbfields[s]; exists && fieldMeta.Sortable {
					dbname := fieldMeta.DbName
					if _, exists := qSortFieldE[dbname]; !exists {
						qSortFieldE[dbname] = sortdesc
						qSortFieldO = append(qSortFieldO, dbname)
					}
				}
			}
		} else if len(vals) > 0 {
			//autre champs de filtre : field=...
			fieldMeta, exists := dbfields[pname]
			dbname := ""
			if exists {
				dbname = fieldMeta.DbName
			}
			if dbname == "" {
				continue
			}

			sql := ""
			for _, s := range vals {
				s = strings.TrimSpace(s)
				op := ""
				or := false
				pos := strings.Index(s, ":")
				fval := s //val du filtre

				//extact filtre eq:... in:... si préfixé par "o", ce sera pour une condition OR
				//filtre non géré ignoré (eq pare defaut)
				if pos >= 2 && pos <= 5 {
					opu := s[:pos]
					oru := or
					if opu[0:1] == "o" {
						oru = true
						opu = opu[1:]
					}
					if opu == "eq" || opu == "not" || opu == "like" || opu == "in" || opu == "lt" || opu == "lte" || opu == "gt" || opu == "gte" {
						op = opu
						or = oru
						fval = s[pos+1:]
					}
					if (opu == "nu" || opu == "nn") && fval == "" { //field=nu: / nn:  null/notnull
						op = opu
						or = oru
						fval = ""
					}
				}

				//filtre vide en considération que si op précise donnée
				if fval == "" && op == "" {
					continue
				}

				// partie sql
				if sql != "" {
					if or {
						sql += " OR "
					} else {
						sql += " AND "
					}
				}

				// TODO : gestion du cas du "deleted_at" : bool
				switch op {
				case "not":
					sql += dbname + " <> ?"
				case "like":
					sql += dbname + " like ?" //% a fournir par le client
				case "gt":
					sql += dbname + " > ?"
				case "gte":
					sql += dbname + " >= ?"
				case "lt":
					sql += dbname + " < ?"
				case "lte":
					sql += dbname + " <= ?"
				case "in": //spe virgule
					inlist := ""
					inarr := strings.Split(fval, ",")
					for _, i := range inarr {
						if i != "" {
							pval := fieldMeta.StringToType(i)
							if pval != nil {
								inlist += "?,"
								pParamsVals = append(pParamsVals, pval)
							}
						}
					}
					if inlist != "" {
						sql += dbname + " in (" + inlist[:len(inlist)-1] + ")"
					}
				case "nu": //null
					sql += dbname + " is null"
				case "nn": //not null
					sql += dbname + " is not null"
				default: //eq
					sql += dbname + " = ?"
				}

				//param associé
				if op != "in" {
					pval := fieldMeta.StringToType(fval)
					if pval != nil {
						if op == "like" {
							pParamsVals = append(pParamsVals, "%"+*(pval.(*string))+"%")
						} else {
							pParamsVals = append(pParamsVals, pval)
						}
					} else {
						sql = ""
					}
				}
			}

			//ajout filtre
			if sql != "" {
				pQueryFieldFilter = append(pQueryFieldFilter, sql)
			}
		}
	}

	// Construction trie
	sort := ""
	for _, s := range qSortFieldO {
		if sort != "" {
			sort += ", "
		}
		sort += ", " + s
		if qSortFieldE[s] {
			sort += " DESC"
		} else {
			sort += " ASC"
		}
	}
	if sort == "" {
		//tri par defaut
		for _, f := range dbfields {
			if f.Sortable && strings.HasSuffix(f.DbName, ".id") {
				sort = f.DbName + " ASC"
				break
			}
		}
	}

	//requete
	where := ""
	for _, q := range pQueryFieldFilter {
		if where != "" {
			where += " AND "
		}
		where += "(" + q + ")"
	}

	//paging
	if qLimit <= 0 {
		qLimit = DefaultRecordPerPage
	}
	if qLimit > MaxRecordPerPage {
		qLimit = MaxRecordPerPage
	}
	if qPage > 0 {
		//n° de page demandé, on déduit l'offset
		qOffset = (qPage - 1) * qLimit
	}
	if qOffset < 0 {
		qOffset = 0
	}

	return SearchQuery{
		Offset:    qOffset,
		Limit:     qLimit,
		SQLSort:   sort,
		SQLFilter: where,
		SQLParams: pParamsVals,
	}
}

// paramSearchQuery reprsente les infos d'un parametre acceptable pour la recherche
type paramSearchQuery struct {
	Searchable bool
	Sortable   bool
	APIName    string //requete partie order by
	DbName     string //requete partie order by
	Type       string
}

//StringToType helper convertion de type
func (c paramSearchQuery) StringToType(input string) interface{} {
	var ret interface{}
	switch c.Type {
	case "date":
		if len(input) >= 10 {
			//format 2006-01-02 attendu, le reste est ignoré
			v, e := time.Parse("2006-01-02", input[:11])
			if e == nil {
				ret = &v
			}
		}
	case "time":
		if len(input) >= 8 {
			//format 15:04:05
			v, e := time.Parse("15:04:05", input[:9])
			if e == nil {
				ret = &v
			}
		} else if len(input) >= 5 {
			//format 15:04
			v, e := time.Parse("15:04", input[:6])
			if e == nil {
				ret = &v
			}
		}
	case "datetime":
		if len(input) >= 25 {
			//format "2006-01-02T15:04:05Z07:00"
			v, e := time.Parse("2006-01-02T15:04:05Z07:00", input[:26])
			if e == nil {
				ret = &v
			}
		} else if len(input) >= 19 {
			//format "2006-01-02T15:04:05"
			v, e := time.Parse("2006-01-02T15:04:05", input[:20])
			if e == nil {
				ret = &v
			}
		} else if len(input) >= 10 {
			//format "2006-01-02"
			v, e := time.Parse("2006-01-02T15:04:05Z07:00", input[:11])
			if e == nil {
				ret = &v
			}
		}
	case "int":
		v, e := strconv.Atoi(input)
		if e == nil {
			ret = &v
		}
	case "uint":
		v, e := strconv.Atoi(input)
		if e == nil && v > 0 {
			ret = &v
		}
	case "bool": //truie/false/0/1
		if strings.EqualFold(input, "true") || strings.EqualFold(input, "1") {
			//b := true //la bdd utile des int pour les boolean
			b := 1
			ret = &b
		} else if strings.EqualFold(input, "false") || strings.EqualFold(input, "0") {
			b := 0
			ret = &b
		}
	case "float":
		v, e := strconv.ParseFloat(input, 64)
		if e == nil {
			ret = &v
		}
	default:
		ret = &input
	}
	return ret
}

// extractSearchInfo extraits les capa de filtrage annoté sur la structure fourni
func extractSearchInfo(v interface{}) map[string]*paramSearchQuery {
	params := make(map[string]*paramSearchQuery)

	typ := reflect.TypeOf(v).Elem()

	for i := 0; i < typ.NumField(); i++ {
		//par defaut info repris de la structure
		name := typ.Field(i).Name
		dbname := name
		apiname := name
		dbtype := ""
		searcheable := false
		sortable := false

		tagAU := strings.Split(typ.Field(i).Tag.Get("apiuse"), ",")
		for _, a := range tagAU {
			if a == "search" {
				searcheable = true
			}
			if a == "sort" {
				sortable = true
			}
		}

		if !searcheable && !sortable {
			continue //non déclaré utilisable en tant que filtre
		}

		kd := typ.Field(i).Type.Kind()
		switch kd {
		case reflect.Struct:
			if typ.Field(i).Type.Name() == "Time" {
				dbtype = "datetime"
			}
		case reflect.Bool:
			dbtype = "bool"
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			dbtype = "int"
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			dbtype = "uint"
		case reflect.Float32, reflect.Float64:
			dbtype = "float"
		case reflect.String:
			dbtype = "string"
		}

		//surcharge tag
		tagDbName := typ.Field(i).Tag.Get("dbfield")
		if tagDbName != "" {
			dbname = tagDbName
		}
		tagAPIName := typ.Field(i).Tag.Get("apiname")
		if tagAPIName != "" {
			apiname = tagAPIName
		}
		tagDbType := typ.Field(i).Tag.Get("dbtype")
		if tagDbType != "" {
			dbtype = tagDbType
		}
		apiname = strings.TrimSpace(strings.ToLower(apiname))
		if apiname == "" {
			continue //apiname volontairement spec à vide
		}

		params[apiname] = &paramSearchQuery{
			Searchable: searcheable,
			Sortable:   sortable,
			APIName:    apiname,
			DbName:     dbname,
			Type:       dbtype,
		}
	}

	return params
}
