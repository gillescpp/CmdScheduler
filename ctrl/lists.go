package ctrl

import (
	"CmdScheduler/dal"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

// LabelListInt model vue standard pour les valeurs id = libellé
type LabelListInt struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// LabelListStr model vue standard pour les valeurs id = libellé
type LabelListStr struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

//apiRightLevelList liste des droits
func apiRightLevelList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	lst := []LabelListInt{
		{
			ID:   dal.RightLvlAdmin,
			Name: "Admin",
		},
		{
			ID:   dal.RightLvlTaskBuilder,
			Name: "Task Builder",
		},
		{
			ID:   dal.RightLvlTaskRunner,
			Name: "Task Runner",
		},
		{
			ID:   dal.RightLvlViewer,
			Name: "Task Viewer",
		},
	}

	//retour ok
	writeStdJSONResp(w, http.StatusOK, lst)
}

//apiTaskTypeList liste des type d'applis
func apiTaskTypeList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	lst := []LabelListStr{
		{
			ID:   "CmdTask",
			Name: "Command app",
		},
		{
			ID:   "URLCheckTask",
			Name: "Check URL",
		},
	}

	//retour ok
	writeStdJSONResp(w, http.StatusOK, lst)
}
