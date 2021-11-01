package schd

import (
	"CmdScheduler/dal"
	"CmdScheduler/slog"
	"fmt"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func InitWorker(t *testing.T) {
	slog.InitLogs("", 0, 0, false)

	viper.SetDefault("http_port", 8100)
	viper.SetDefault("Title", "Cmd Scheduler")
	viper.SetDefault("db_driver", "sqlite3")
	viper.SetDefault("db_datasource", "file:data.db")
	viper.SetDefault("db_prefix", "SCHED")

	dal.InitDb(viper.GetString("db_driver"), viper.GetString("db_datasource"), viper.GetString("db_prefix"))

}

// TestHelloName calls greetings.Hello with a name, checking
// for a valid return value.
func TestWorker(t *testing.T) {
	InitWorker(t)

	//instance worker
	initQueue := make(map[int]*dal.DbQueue)
	initQueue[1] = &dal.DbQueue{
		ID:      1,
		Lib:     "Q1",
		Slot:    2,
		MaxSize: 500,
	}
	initQueue[2] = &dal.DbQueue{
		ID:      2,
		Lib:     "Q2",
		Slot:    1,
		MaxSize: 500,
	}

	w := NewWorker(initQueue)

	//boot
	w.Start()

	//lancement de taches
	tf := PreparedTF{
		TFID:  0,
		TFLib: "",
		Ident: "",
		DtRef: time.Now(),
		State: StateNew,
		Detail: []PreparedDetail{
			{
				DbTaskFlowDetail: dal.DbTaskFlowDetail{
					Idx:            1,
					TaskID:         1,
					NextTaskIDOK:   0,
					NextTaskIDFail: -1,
					RetryIfFail:    0,
				},
				Agent: dal.DbAgent{
					ID:     1,
					Host:   "test",
					APIKey: "test",
				},
				Task: dal.DbTask{
					ID:      1,
					Lib:     "test",
					Type:    "none",
					Timeout: 1000,
					Args:    []string{},
					ExecOn:  []int{},
					Info:    "",
				},
				AgentSID: 0,
			},
		},
		NamedArgs:    map[string]string{},
		LaunchSource: "",
		ErrMngt:      0,
		QueueID:      0,
		QueueLib:     "",
	}

	totalpush := 0
	pushend := false
	go func() {
		for m := 1; m < 3; m++ {
			for i := 1; i < 30; i++ {
				tf.TFLib = fmt.Sprintf("TEST %v", i)
				tf.Ident = fmt.Sprintf("TF%v-%v", m, i)

				//queue tour à tour 0,1,2
				if tf.QueueID > len(initQueue) {
					tf.QueueID = 1
				}
				tf.QueueLib = ""
				if tf.QueueID > 0 {
					tf.QueueLib = fmt.Sprintf("Q%v", tf.QueueID)
				}

				w.AppendTF(tf)
				tf.QueueID++
				totalpush++
			}
			time.Sleep(1 * time.Second)
		}
		pushend = true
	}()

	//time.Sleep(2 * time.Second)
	for i := 1; i < 100; i++ {
		state := w.GetLastState()
		info := fmt.Sprintf("TOTAL PUSH %v\r\n", totalpush)
		totalterminated := 0
		for _, q := range state.QueueState {
			info += fmt.Sprintf("%v : processing=%v, waiting=%v, total terminated=%v, total launched=%v\r\n",
				q.Lib, q.Processing, q.Waiting, q.Terminated, q.Launched)
			totalterminated += q.Terminated
		}
		fmt.Println(info)
		time.Sleep(500 * time.Millisecond)
		if pushend && totalterminated >= totalpush {
			fmt.Println("seems terminated")
			break
		}
	}
	w.Stop()
}
