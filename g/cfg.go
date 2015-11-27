package g

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/toolkits/file"
)

type HttpConfig struct {
	Enable bool   `json:"enable"`
	Listen string `json:"listen"`
}

type ZkConfig struct { //drrs
	Ip      string `json:"ip"`
	Addr    string `json:"addr"`
	Timeout int    `json:"timeout"`
}

type DrrsConfig struct { //drrs
	Enabled  bool      `json:"enabled"`
	UseZk    bool      `json:"useZk"`
	Dest     string    `json:"dest"`
	Replicas int32     `json:"replicas"`
	MaxIdle  int32     `json:"maxIdle"`
	Zk       *ZkConfig `json:"zk"`
}

type GraphConfig struct {
	ConnTimeout int32             `json:"connTimeout"`
	CallTimeout int32             `json:"callTimeout"`
	MaxConns    int32             `json:"maxConns"`
	MaxIdle     int32             `json:"maxIdle"`
	Replicas    int32             `json:"replicas"`
	Cluster     map[string]string `json:"cluster"`
}

type GlobalConfig struct {
	Debug string       `json:"debug"`
	Http  *HttpConfig  `json:"http"`
	Graph *GraphConfig `json:"graph"`
	Drrs  *DrrsConfig  `json:"drrs"` //drrs
}

var (
	ConfigFile string
	config     *GlobalConfig
	configLock = new(sync.RWMutex)
)

func Config() *GlobalConfig {
	configLock.RLock()
	defer configLock.RUnlock()
	return config
}

func ParseConfig(cfg string) {
	if cfg == "" {
		log.Fatalln("config file not specified: use -c $filename")
	}

	if !file.IsExist(cfg) {
		log.Fatalln("config file specified not found:", cfg)
	}

	ConfigFile = cfg

	configContent, err := file.ToTrimString(cfg)
	if err != nil {
		log.Fatalln("read config file", cfg, "error:", err.Error())
	}

	var c GlobalConfig
	err = json.Unmarshal([]byte(configContent), &c)
	if err != nil {
		log.Fatalln("parse config file", cfg, "error:", err.Error())
	}

	// set config
	configLock.Lock()
	defer configLock.Unlock()
	config = &c

	log.Println("g.ParseConfig ok, file", cfg)
}
