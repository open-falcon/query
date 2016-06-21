package conf

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"reflect"
	"sync"

	"github.com/Cepave/query/utils"
)

type Gconfig struct {
	Funcations []FunConfig
}

type FunConfig struct {
	FuncationName string   `json:"funcation_name"`
	FilePath      string   `json:"file_path"`
	Params        []string `json:"params"`
	Description   string   `json:"description"`
	Codes         string   `json:"-"`
}

var (
	gconfig     []*FunConfig
	configLock  = new(sync.RWMutex)
	confpath    *string
	FunctionMap map[string]*FunConfig
)

func Config() map[string]*FunConfig {
	configLock.RLock()
	defer configLock.RUnlock()
	return FunctionMap
}

func functionMapGen() {
	FunctionMap = map[string]*FunConfig{}
	for _, v := range gconfig {
		contain := jsFileReader(v.FilePath)
		v.Codes = contain
		FunctionMap[v.FuncationName] = v
	}
}

func ReadConf(f string) {
	if f == "" {
		f = "./lambdaSetup.json"
	}
	confpath = &f
	dat, err := ioutil.ReadFile(f)
	if err != nil {
		log.Println(err)
	}
	var myconf []*FunConfig
	json.Unmarshal(dat, &myconf)
	if len(myconf) == 0 {
		log.Println("conf file is empty or format is wrong, please check it!")
	}
	gconfig = myconf
	functionMapGen()
}

func Reload() {
	configLock.RLock()
	ReadConf(*confpath)
	defer configLock.RUnlock()
}

func GetFunc(key string) *FunConfig {
	return FunctionMap[key]
}

func GetAvaibleFun() []string {
	return utils.GetMapKeys(reflect.ValueOf(FunctionMap).MapKeys())
}
