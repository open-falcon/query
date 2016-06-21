package http

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"github.com/Cepave/query/g"
	"github.com/astaxie/beego/orm"
	"github.com/bitly/go-simplejson"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"time"
)

type Host struct {
	Id             int
	Hostname       string
	Ip             string
	Agent_version  string
	Plugin_version string
	Maintain_begin int64
	Maintain_end   int64
	Update_at      string
}

type Grp struct {
	Id          int
	Grp_name    string
	Create_user string
	Create_at   string
	Come_from   int
}

type Tpl struct {
	Id          int
	Tpl_name    string
	Parent_id   int
	Action_id   int
	Create_user string
	Create_at   string
}

type Grp_tpl struct {
	Id        int
	Grp_id    int
	Tpl_id    int
	Bind_user string
}

type Grp_host struct {
	Id      int
	Grp_id  int
	Host_id int
}

type Plugin_dir struct {
	Id          int
	Grp_id      int
	Dir         string
	Create_user string
}

/**
 * @function name:   func getNow() string
 * @description:     This function gets string of current time.
 * @related issues:  OWL-093
 * @param:           void
 * @return:          now sting
 * @author:          Don Hsieh
 * @since:           10/21/2015
 * @last modified:   10/21/2015
 * @called by:       func hostCreate(nodes map[string]interface{})
 *                   func hostgroupCreate(nodes map[string]interface{})
 *                   func templateCreate(nodes map[string]interface{})
 *                   func hostUpdate(nodes map[string]interface{})
 *                   func setResponse(rw http.ResponseWriter, resp map[string]interface{})
 */
func getNow() string {
	t := time.Now()
	now := t.Format("2006-01-02 15:04:05")
	return now
}

/**
 * @function name:   func getHostId(params map[string]interface{}) string
 * @description:     This function gets host ID.
 * @related issues:  OWL-240
 * @param:           params map[string]interface{}
 * @return:          hostId string
 * @author:          Don Hsieh
 * @since:           12/16/2015
 * @last modified:   12/16/2015
 * @called by:       func checkHostExist(params map[string]interface{}, result map[string]interface{}) Host
 */
func getHostId(params map[string]interface{}) string {
	hostId := ""
	if val, ok := params["hostid"]; ok {
		if val != nil {
			hostId = val.(string)
		}
	}
	return hostId
}

/**
 * @function name:   func getHostName(params map[string]interface{}) string
 * @description:     This function gets host name.
 * @related issues:  OWL-240
 * @param:           params map[string]interface{}
 * @return:          hostName string
 * @author:          Don Hsieh
 * @since:           12/16/2015
 * @last modified:   01/19/2015
 * @called by:       func checkHostExist(params map[string]interface{}, result map[string]interface{}) Host
 *                   func addHost(params map[string]interface{}, args map[string]string, result map[string]interface{})
 */
func getHostName(params map[string]interface{}) string {
	hostName := ""
	if val, ok := params["host"]; ok {
		if val != nil {
			hostName = val.(string)
		}
	} else if val, ok = params["name"]; ok {
		if val != nil {
			hostName = val.(string)
		}
	}
	return hostName
}

/**
 * @function name:   func checkHostExist(params map[string]interface{}, result map[string]interface{}) Host
 * @description:     This function checks if a host existed.
 * @related issues:  OWL-262, OWL-257, OWL-240
 * @param:           params map[string]interface{}
 * @param:           result map[string]interface{}
 * @return:          host Host
 * @author:          Don Hsieh
 * @since:           12/16/2015
 * @last modified:   01/06/2016
 * @called by:       func hostCreate(nodes map[string]interface{})
 *                   func hostUpdate(nodes map[string]interface{})
 */
func checkHostExist(params map[string]interface{}, result map[string]interface{}) Host {
	var host Host
	o := orm.NewOrm()
	hostId := getHostId(params)
	hostName := getHostName(params)
	if hostId != "" {
		hostIdint, err := strconv.Atoi(hostId)
		if err != nil {
			setError(err.Error(), result)
		} else {
			host := Host{Id: hostIdint}
			err := o.Read(&host)
			if err != nil {
				setError(err.Error(), result)
			}
		}
	} else {
		err := o.QueryTable("host").Filter("hostname", hostName).One(&host)
		if err == orm.ErrMultiRows {
			// Have multiple records
			log.Println("returned multiple rows")
		} else if err == orm.ErrNoRows {
			// No result
		}
	}
	return host
}

/**
 * @function name:   func setError(error string, result map[string]interface{})
 * @description:     This function sets error message.
 * @related issues:  OWL-257
 * @param:           error string
 * @param:           result map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           01/01/2016
 * @last modified:   01/01/2016
 * @called by:       func bindGroup(hostId int64, params map[string]interface{}, args map[string]string, result map[string]interface{})
 *                   func bindTemplate(params map[string]interface{}, args map[string]string, result map[string]interface{})
 *                   func addHost(hostName string, params map[string]interface{}, args map[string]string, result map[string]interface{})
 *                   func hostCreate(nodes map[string]interface{})
 */
func setError(error string, result map[string]interface{}) {
	log.Println("Error =", error)
	result["error"] = append(result["error"].([]string), error)
}

/**
 * @function name:   func bindGroup(hostId int64, params map[string]interface{}, args map[string]string, result map[string]interface{})
 * @description:     This function binds a host to a host group.
 * @related issues:  OWL-262, OWL-257, OWL-240
 * @param:           hostId int64
 * @param:           params map[string]interface{}
 * @param:           args map[string]string
 * @param:           result map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           12/15/2015
 * @last modified:   01/06/2016
 * @called by:       func hostUpdate(nodes map[string]interface{})
 *                   func addHost(hostName string, params map[string]interface{}, args map[string]string, result map[string]interface{})
 */
func bindGroup(hostId int, params map[string]interface{}, args map[string]string, result map[string]interface{}) {
	if _, ok := params["groups"]; ok {
		o := orm.NewOrm()
		groupId := ""
		var grp_host Grp_host
		groups := params["groups"].([]interface{})
		for _, group := range groups {
			groupId = group.(map[string]interface{})["groupid"].(string)
			args["groupId"] = groupId
			grp_id, err := strconv.Atoi(groupId)

			sqlcmd := "SELECT * FROM falcon_portal.grp_host WHERE host_id=? AND grp_id=?"
			err = o.Raw(sqlcmd, hostId, grp_id).QueryRow(&grp_host)
			if err == orm.ErrNoRows {
				// No result
				grp_host := Grp_host{
					Grp_id:  grp_id,
					Host_id: int(hostId),
				}
				log.Println("grp_host =", grp_host)
				_, err = o.Insert(&grp_host)
				if err != nil {
					setError(err.Error(), result)
				}
			} else if err != nil {
				setError(err.Error(), result)
			} else {
				log.Println("grp_host existed =", grp_host)
			}
		}
	}
}

/**
 * @function name:   func bindTemplate(params map[string]interface{}, args map[string]string, result map[string]interface{})
 * @description:     This function binds a host to a template.
 * @related issues:  OWL-262, OWL-257, OWL-240
 * @param:           params map[string]interface{}
 * @param:           args map[string]string
 * @param:           result map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           12/15/2015
 * @last modified:   01/06/2016
 * @called by:       func hostUpdate(nodes map[string]interface{})
 *                   func addHost(hostName string, params map[string]interface{}, args map[string]string, result map[string]interface{})
 */
func bindTemplate(params map[string]interface{}, args map[string]string, result map[string]interface{}) {
	if _, ok := params["templates"]; ok {
		o := orm.NewOrm()
		groupId := args["groupId"]
		grp_id, _ := strconv.Atoi(groupId)
		var grp_tpl Grp_tpl
		templates := params["templates"].([]interface{})
		for _, template := range templates {
			templateId := template.(map[string]interface{})["templateid"].(string)
			tpl_id, err := strconv.Atoi(templateId)
			args["templateId"] = templateId

			sqlcmd := "SELECT * FROM falcon_portal.grp_tpl WHERE grp_id=? AND tpl_id=?"
			err = o.Raw(sqlcmd, grp_id, tpl_id).QueryRow(&grp_tpl)
			if err == orm.ErrNoRows {
				grp_tpl := Grp_tpl{
					Grp_id:    grp_id,
					Tpl_id:    tpl_id,
					Bind_user: "zabbix",
				}
				log.Println("grp_tpl =", grp_tpl)
				_, err = o.Insert(&grp_tpl)
				if err != nil {
					setError(err.Error(), result)
				}
			} else if err != nil {
				setError(err.Error(), result)
			} else {
				log.Println("grp_tpl existed =", grp_tpl)
			}
		}
	}
}

/**
 * @function name:   func checkInputFormat(params map[string]interface{}, result map[string]interface{}) bool
 * @description:     This function checks input format.
 * @related issues:  OWL-262
 * @param:           params map[string]interface{}
 * @param:           result map[string]interface{}
 * @return:          valid bool
 * @author:          Don Hsieh
 * @since:           01/06/2016
 * @last modified:   01/06/2016
 * @called by:       func addHost(params map[string]interface{}, args map[string]string, result map[string]interface{})
 *                   func hostUpdate(nodes map[string]interface{})
 */
func checkInputFormat(params map[string]interface{}, result map[string]interface{}) bool {
	valid := true
	if val, ok := params["interfaces"]; ok {
		if reflect.TypeOf(val) != reflect.TypeOf([]interface{}{}) {
			setError("interfaces shall be an array of objects [{}]", result)
			valid = false
		}
	}
	if val, ok := params["groups"]; ok {
		if reflect.TypeOf(val) != reflect.TypeOf([]interface{}{}) {
			setError("groups shall be an array of objects [{}]", result)
			valid = false
		}
	}
	if val, ok := params["templates"]; ok {
		if reflect.TypeOf(val) != reflect.TypeOf([]interface{}{}) {
			setError("templates shall be an array of objects [{}]", result)
			valid = false
		}
	}
	return valid
}

/**
 * @function name:   func addHost(params map[string]interface{}, args map[string]string, result map[string]interface{})
 * @description:     This function inserts a host to "host" table and binds the host to its group and template.
 * @related issues:  OWL-262, OWL-257, OWL-240
 * @param:           params map[string]interface{}
 * @param:           args map[string]string
 * @param:           result map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           12/21/2015
 * @last modified:   01/06/2016
 * @called by:       func hostCreate(nodes map[string]interface{})
 *                   func hostUpdate(nodes map[string]interface{})
 */
func addHost(params map[string]interface{}, args map[string]string, result map[string]interface{}) {
	hostName := getHostName(params)
	if len(hostName) == 0 {
		setError("host name can not be null.", result)
	} else {
		valid := checkInputFormat(params, result)
		if valid {
			args["host"] = hostName
			ip := ""
			if _, ok := params["interfaces"]; ok {
				interfaces := params["interfaces"].([]interface{})
				for i, arg := range interfaces {
					if i == 0 {
						if val, ok := arg.(map[string]interface{})["ip"]; ok {
							ip = val.(string)
							args["ip"] = ip
						}
					}
				}
			}
			host := Host{
				Hostname:  hostName,
				Ip:        ip,
				Update_at: getNow(),
			}
			log.Println("host =", host)

			o := orm.NewOrm()
			hostId, err := o.Insert(&host)
			if err != nil {
				setError(err.Error(), result)
			} else {
				bindGroup(int(hostId), params, args, result)
				hostid := strconv.Itoa(int(hostId))
				hostids := [1]string{string(hostid)}
				result["hostids"] = hostids
				bindTemplate(params, args, result)
			}
		}
	}
}

/**
 * @function name:   func hostCreate(nodes map[string]interface{})
 * @description:     This function gets host data for database insertion.
 * @related issues:  OWL-262
 * @related issues:  OWL-257, OWL-240, OWL-093, OWL-086, OWL-085
 * @param:           nodes map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           09/11/2015
 * @last modified:   01/06/2016
 * @called by:       func apiParser(rw http.ResponseWriter, req *http.Request)
 */
func hostCreate(nodes map[string]interface{}) {
	log.Println("func hostCreate()")
	params := nodes["params"].(map[string]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors

	host := checkHostExist(params, result)
	if host.Id > 0 {
		setError("host name existed: "+host.Hostname, result)
	} else {
		args := map[string]string{}
		addHost(params, args, result)
		if _, ok := params["inventory"]; ok {
			inventory := params["inventory"].(map[string]interface{})
			macAddr := inventory["macaddress_a"].(string) + inventory["macaddress_b"].(string)
			args["macAddr"] = macAddr
		}
		log.Println("args =", args)
	}
	nodes["result"] = result
}

/**
 * @function name:   func unbindGroup(hostId string, result map[string]interface{})
 * @description:     This function unbinds a host to a host group.
 * @related issues:  OWL-241
 * @param:           hostId string
 * @param:           result map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           01/01/2016
 * @last modified:   01/01/2016
 * @called by:       func removeHost(hostIds []string, result map[string]interface{})
 */
func unbindGroup(hostId string, result map[string]interface{}) {
	o := orm.NewOrm()
	sql := "DELETE FROM grp_host WHERE host_id = ?"
	res, err := o.Raw(sql, hostId).Exec()
	if err != nil {
		setError(err.Error(), result)
	}
	num, _ := res.RowsAffected()
	log.Println("mysql row affected nums =", num)
}

/**
 * @function name:   func removeHost(hostIds []string, result map[string]interface{})
 * @description:     This function deletes host from "host" table.
 * @related issues:  OWL-262, OWL-241
 * @param:           hostIds []string
 * @param:           result map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           01/01/2016
 * @last modified:   01/06/2016
 * @called by:       func hostDelete(nodes map[string]interface{})
 */
func removeHost(hostIds []string, result map[string]interface{}) {
	o := orm.NewOrm()
	hostids := []string{}
	for _, hostId := range hostIds {
		if id, err := strconv.Atoi(hostId); err == nil {
			num, err := o.Delete(&Host{Id: id})
			if err != nil {
				setError(err.Error(), result)
			} else {
				if num > 0 {
					log.Println("RowsDeleted =", num)
					unbindGroup(hostId, result)
					hostids = append(hostids, hostId)
				}
			}
		}
	}
	result["hostids"] = hostids
}

/**
 * @function name:   func hostDelete(nodes map[string]interface{})
 * @description:     This function handles host.delete API requests.
 * @related issues:  OWL-257, OWL-241, OWL-093, OWL-086, OWL-085
 * @param:           nodes map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           09/11/2015
 * @last modified:   01/01/2016
 * @called by:       func apiParser(rw http.ResponseWriter, req *http.Request)
 */
func hostDelete(nodes map[string]interface{}) {
	params := nodes["params"].([]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors

	hostIds := []string{}
	hostId := ""
	for _, param := range params {
		if val, ok := param.(map[string]interface{})["host_id"]; ok {
			if val != nil {
				hostId = string(val.(json.Number))
				hostIds = append(hostIds, hostId)
			}
		}
	}
	removeHost(hostIds, result)
	nodes["result"] = result
}

/**
 * @function name:   func getGroups(hostId string) []interface{}
 * @description:     This function gets hostgroups which contain the host ID.
 * @related issues:  OWL-254
 * @param:           hostId string
 * @return:          groups []interface{}
 * @author:          Don Hsieh
 * @since:           01/12/2016
 * @last modified:   01/12/2016
 * @called by:       func hostGet(nodes map[string]interface{})
 */
func getGroups(hostId string) []interface{} {
	groups := []interface{}{}
	o := orm.NewOrm()
	var grp_ids []int
	o.Raw("SELECT grp_id FROM falcon_portal.grp_host WHERE host_id=?", hostId).QueryRows(&grp_ids)
	for _, grp_id := range grp_ids {
		groupId := strconv.Itoa(grp_id)
		group := map[string]string{
			"groupid": groupId,
		}
		groups = append(groups, group)
	}
	return groups
}

/**
 * @function name:   func hostGet(nodes map[string]interface{})
 * @description:     This function gets existed host data.
 * @related issues:  OWL-283, OWL-262, OWL-257, OWL-254
 * @param:           nodes map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           12/29/2015
 * @last modified:   01/19/2016
 * @called by:       func apiParser(rw http.ResponseWriter, req *http.Request)
 */
func hostGet(nodes map[string]interface{}) {
	log.Println("func hostGet()")
	params := nodes["params"].(map[string]interface{})
	items := []interface{}{}
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	hostNames := []string{}
	queryAll := false
	if val, ok := params["filter"]; ok {
		filter := val.(map[string]interface{})
		if val, ok = filter["host"]; ok {
			for _, hostName := range val.([]interface{}) {
				if hostName.(string) == "_all_" {
					queryAll = true
				} else {
					hostNames = append(hostNames, hostName.(string))
				}
			}
		}
	}
	o := orm.NewOrm()
	hostId := ""
	groups := []interface{}{}
	countOfRows := 0
	if queryAll {
		var hosts []*Host
		num, err := o.Raw("SELECT * FROM falcon_portal.host").QueryRows(&hosts)
		if err != nil {
			setError(err.Error(), result)
		} else {
			countOfRows = int(num)
			log.Println("countOfRows =", countOfRows)
			for _, host := range hosts {
				item := map[string]interface{}{}
				hostId = strconv.Itoa(host.Id)
				groups = getGroups(hostId)
				item["hostid"] = hostId
				item["hostname"] = host.Hostname
				item["ip"] = host.Ip
				item["groups"] = groups
				items = append(items, item)
			}
		}
	} else {
		var host Host
		for _, hostName := range hostNames {
			item := map[string]interface{}{}
			hostId = ""
			err := o.QueryTable("host").Filter("hostname", hostName).One(&host)
			if err == orm.ErrMultiRows {
				setError("returned multiple rows", result)
			} else if err == orm.ErrNoRows {
				log.Println("host not found")
			} else if host.Id > 0 {
				hostId = strconv.Itoa(host.Id)
				groups = getGroups(hostId)
				countOfRows++
			}
			item["hostid"] = hostId
			item["hostname"] = hostName
			item["ip"] = host.Ip
			item["groups"] = groups
			items = append(items, item)
		}
	}
	result["items"] = items
	result["count"] = countOfRows
	nodes["result"] = result
}

func muteAlertsOfHost(host Host, params map[string]interface{}, result map[string]interface{}) Host {
	if val, ok := params["mute"]; ok {
		if reflect.TypeOf(val) != reflect.TypeOf("") {
			setError("value of mute shall be a string of 1 or 0", result)
		} else {
			if val == "1" {
				host.Maintain_begin = int64(946684800) // Sat, 01 Jan 2000 00:00:00 GMT
				host.Maintain_end = int64(4292329420)  // Thu, 07 Jan 2106 17:43:40 GMT
			} else if val == "0" {
				host.Maintain_begin = int64(0)
				host.Maintain_end = int64(0)
			}
		}
	}
	return host
}

/**
 * @function name:   func hostUpdate(nodes map[string]interface{})
 * @description:     This function updates host data.
 * @related issues:  OWL-262, OWL-257, OWL-240, OWL-093, OWL-086
 * @param:           nodes map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           09/23/2015
 * @last modified:   01/06/2016
 * @called by:       func apiParser(rw http.ResponseWriter, req *http.Request)
 */
func hostUpdate(nodes map[string]interface{}) {
	log.Println("func hostUpdate()")
	params := nodes["params"].(map[string]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	args := map[string]string{}
	host := checkHostExist(params, result)
	if host.Id > 0 {
		log.Println("host existed")
		valid := checkInputFormat(params, result)
		if valid {
			host = muteAlertsOfHost(host, params, result)
			host.Update_at = getNow()
			o := orm.NewOrm()
			num, err := o.Update(&host)
			if err != nil {
				setError(err.Error(), result)
			} else {
				log.Println("update hostId =", host.Id)
				log.Println("mysql row affected nums =", num)
				hostid := strconv.Itoa(host.Id)
				if _, ok := params["groups"]; ok {
					unbindGroup(hostid, result)
					bindGroup(host.Id, params, args, result)
				}
				bindTemplate(params, args, result)
				hostids := [1]string{string(hostid)}
				result["hostids"] = hostids
			}
		}
	} else {
		log.Println("host not existed")
		addHost(params, args, result)
	}
	log.Println("args =", args)
	nodes["result"] = result
}

/**
 * @function name:   func hostgroupCreate(nodes map[string]interface{})
 * @description:     This function gets hostgroup data for database insertion.
 * @related issues:  OWL-257, OWL-093, OWL-086
 * @param:           nodes map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           09/21/2015
 * @last modified:   01/01/2016
 * @called by:       func apiParser(rw http.ResponseWriter, req *http.Request)
 */
func hostgroupCreate(nodes map[string]interface{}) {
	log.Println("func hostgroupCreate()")
	params := nodes["params"].(map[string]interface{})
	hostgroupName := params["name"].(string)
	user := "zabbix"
	now := getNow()

	o := orm.NewOrm()
	grp := Grp{
		Grp_name:    hostgroupName,
		Create_user: user,
		Create_at:   now,
	}
	log.Println("grp =", grp)
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	id, err := o.Insert(&grp)
	if err != nil {
		setError(err.Error(), result)
	} else {
		groupid := strconv.Itoa(int(id))
		groupids := [1]string{string(groupid)}
		result["groupids"] = groupids
	}
	nodes["result"] = result
}

/**
 * @function name:   func hostgroupDelete(nodes map[string]interface{})
 * @description:     This function handles hostgroup.delete API requests.
 * @related issues:  OWL-257, OWL-093, OWL-086
 * @param:           nodes map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           09/21/2015
 * @last modified:   01/01/2016
 * @called by:       func apiParser(rw http.ResponseWriter, req *http.Request)
 */
func hostgroupDelete(nodes map[string]interface{}) {
	log.Println("func hostgroupDelete()")
	params := nodes["params"].([]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors

	args := []interface{}{}
	args = append(args, "DELETE FROM falcon_portal.grp WHERE id=?")
	args = append(args, "DELETE FROM falcon_portal.grp_host WHERE grp_id=?")
	args = append(args, "DELETE FROM falcon_portal.grp_tpl WHERE grp_id=?")
	args = append(args, "DELETE FROM falcon_portal.plugin_dir WHERE grp_id=?")
	log.Println("args =", args)

	o := orm.NewOrm()
	groupids := []string{}
	for _, sqlcmd := range args {
		for _, hostgroupId := range params {
			res, err := o.Raw(sqlcmd.(string), hostgroupId).Exec()
			if err != nil {
				setError(err.Error(), result)
			} else {
				num, _ := res.RowsAffected()
				if num > 0 && sqlcmd == "DELETE FROM falcon_portal.grp WHERE id=?" {
					groupids = append(groupids, hostgroupId.(string))
					log.Println("delete hostgroup id =", hostgroupId)
					log.Println("mysql row affected nums =", num)
				}
			}
		}
	}
	result["groupids"] = groupids
	nodes["result"] = result
}

func getTemplateIdsByGroupId(groupId int, result map[string]interface{}) []string {
	templateIds := []string{}
	o := orm.NewOrm()
	var tpl_ids []int
	sqlcmd := "SELECT DISTINCT tpl_id FROM falcon_portal.grp_tpl WHERE grp_id=?"
	_, err := o.Raw(sqlcmd, groupId).QueryRows(&tpl_ids)
	if err == orm.ErrNoRows {
		log.Println("No templates for groupId:", groupId)
	} else if err != nil {
		setError(err.Error(), result)
	} else {
		for _, tpl_id := range tpl_ids {
			templateId := strconv.Itoa(tpl_id)
			templateIds = append(templateIds, templateId)
		}
	}
	return templateIds
}

func getPluginDirsByGroupId(groupId int, result map[string]interface{}) []string {
	pluginDirs := []string{}
	o := orm.NewOrm()
	sqlcmd := "SELECT DISTINCT dir FROM falcon_portal.plugin_dir WHERE grp_id=?"
	_, err := o.Raw(sqlcmd, groupId).QueryRows(&pluginDirs)
	if err == orm.ErrNoRows {
		log.Println("No plugin dirs for groupId:", groupId)
	} else if err != nil {
		setError(err.Error(), result)
	}
	return pluginDirs
}

/**
 * @function name:   func hostgroupGet(nodes map[string]interface{})
 * @description:     This function gets existed hostgroup data.
 * @related issues:  OWL-257, OWL-254
 * @param:           nodes map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           12/29/2015
 * @last modified:   01/07/2016
 * @called by:       func apiParser(rw http.ResponseWriter, req *http.Request)
 */
func hostgroupGet(nodes map[string]interface{}) {
	log.Println("func hostgroupGet()")
	params := nodes["params"].(map[string]interface{})
	items := []interface{}{}
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	groupNames := []string{}
	queryAll := false
	if val, ok := params["filter"]; ok {
		filter := val.(map[string]interface{})
		if val, ok = filter["name"]; ok {
			for _, groupName := range val.([]interface{}) {
				if groupName.(string) == "_all_" {
					queryAll = true
				} else {
					groupNames = append(groupNames, groupName.(string))
				}
			}
		}
	}
	groupId := ""
	o := orm.NewOrm()
	countOfRows := 0
	if queryAll {
		var grps []*Grp
		num, err := o.QueryTable("grp").All(&grps)
		if err != nil {
			setError(err.Error(), result)
		} else {
			countOfRows = int(num)
			for _, grp := range grps {
				item := map[string]interface{}{}
				item["groupid"] = strconv.Itoa(grp.Id)
				item["groupname"] = grp.Grp_name
				item["templateids"] = getTemplateIdsByGroupId(grp.Id, result)
				item["plugins"] = getPluginDirsByGroupId(grp.Id, result)
				items = append(items, item)
			}
		}
	} else {
		var grp Grp
		for _, groupName := range groupNames {
			item := map[string]interface{}{}
			groupId = ""
			err := o.QueryTable("grp").Filter("grp_name", groupName).One(&grp)
			if err == orm.ErrMultiRows {
				setError("returned multiple rows", result)
			} else if err == orm.ErrNoRows {
				log.Println("host group not found")
			} else if grp.Id > 0 {
				groupId = strconv.Itoa(grp.Id)
				countOfRows++
			}
			item["groupid"] = groupId
			item["groupname"] = groupName
			item["templateids"] = getTemplateIdsByGroupId(grp.Id, result)
			item["plugins"] = getPluginDirsByGroupId(grp.Id, result)
			items = append(items, item)
		}
	}
	result["items"] = items
	result["count"] = countOfRows
	nodes["result"] = result
}

func unbindGroupAndTemplates(groupId string, result map[string]interface{}) {
	o := orm.NewOrm()
	sql := "DELETE FROM grp_tpl WHERE grp_id = ?"
	res, err := o.Raw(sql, groupId).Exec()
	if err != nil {
		setError(err.Error(), result)
	}
	num, _ := res.RowsAffected()
	log.Println("mysql row affected nums =", num)
}

func unbindGroupAndPlugins(groupId int, result map[string]interface{}) {
	o := orm.NewOrm()
	sql := "DELETE FROM plugin_dir WHERE grp_id = ?"
	res, err := o.Raw(sql, groupId).Exec()
	if err != nil {
		setError(err.Error(), result)
	}
	num, _ := res.RowsAffected()
	log.Println("unbindGroupAndPlugins row affected nums =", num)
}

func bindGroupAndPlugins(groupId int, pluginDirs []string, result map[string]interface{}) {
	o := orm.NewOrm()
	var plugin_dir Plugin_dir
	for _, pluginDir := range pluginDirs {
		sqlcmd := "SELECT * FROM falcon_portal.plugin_dir WHERE grp_id=? AND dir=?"
		err := o.Raw(sqlcmd, groupId, pluginDir).QueryRow(&plugin_dir)
		if err == orm.ErrNoRows {
			plugin_dir := Plugin_dir{
				Grp_id:      groupId,
				Dir:         pluginDir,
				Create_user: "zabbix",
			}
			log.Println("plugin_dir =", plugin_dir)
			_, err = o.Insert(&plugin_dir)
			if err != nil {
				setError(err.Error(), result)
			}
		} else if err != nil {
			setError(err.Error(), result)
		} else {
			log.Println("plugin_dir existed =", plugin_dir)
		}
	}
}

/**
 * @function name:   func hostgroupUpdate(nodes map[string]interface{})
 * @description:     This function updates hostgroup data.
 * @related issues:  OWL-257, OWL-093, OWL-086
 * @param:           nodes map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           09/21/2015
 * @last modified:   01/01/2016
 * @called by:       func apiParser(rw http.ResponseWriter, req *http.Request)
 */
func hostgroupUpdate(nodes map[string]interface{}) {
	log.Println("func hostgroupUpdate()")
	params := nodes["params"].(map[string]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors

	if _, ok := params["groupid"]; ok {
		hostgroupId, err := strconv.Atoi(params["groupid"].(string))
		if err != nil {
			setError(err.Error(), result)
		} else {
			o := orm.NewOrm()
			grp := Grp{Id: hostgroupId}
			err := o.Read(&grp)
			if err != nil {
				setError(err.Error(), result)
			}

			if _, ok := params["name"]; ok {
				hostgroupName := params["name"].(string)
				if hostgroupName != "" {
					grp.Grp_name = hostgroupName
					num, err := o.Update(&grp)
					if err != nil {
						setError(err.Error(), result)
					} else if num > 0 {
						groupids := [1]string{strconv.Itoa(hostgroupId)}
						result["groupids"] = groupids
						log.Println("update groupid =", hostgroupId)
						log.Println("mysql row affected nums =", num)
					}
				}
			}

			if _, ok := params["templates"]; ok {
				groupIds := []int{}
				templateIds := []int{}
				groupIds = append(groupIds, hostgroupId)
				templates := params["templates"].([]interface{})
				for _, template := range templates {
					templateId := template.(map[string]interface{})["templateid"].(string)
					templateIdInt, err := strconv.Atoi(templateId)
					if err != nil {
						setError(err.Error(), result)
					}
					templateIds = append(templateIds, templateIdInt)
				}
				unbindGroupAndTemplates(strconv.Itoa(hostgroupId), result)
				bindTemplatesAndGroups(groupIds, templateIds, result)
				groupids := [1]string{strconv.Itoa(hostgroupId)}
				result["groupids"] = groupids
			}

			if _, ok := params["plugins"]; ok {
				pluginDirs := []string{}
				plugins := params["plugins"].([]interface{})
				for _, plugin := range plugins {
					pluginDir := plugin.(map[string]interface{})["plugin"].(string)
					pluginDirs = append(pluginDirs, pluginDir)
				}
				unbindGroupAndPlugins(hostgroupId, result)
				bindGroupAndPlugins(hostgroupId, pluginDirs, result)
				groupids := [1]string{strconv.Itoa(hostgroupId)}
				result["groupids"] = groupids
			}
		}
	} else {
		setError("params['groupid'] must not be empty", result)
	}
	nodes["result"] = result
}

/**
 * @function name:   func templateCreate(nodes map[string]interface{})
 * @description:     This function gets template data for database insertion.
 * @related issues:  OWL-257, OWL-093, OWL-086
 * @param:           nodes map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           09/22/2015
 * @last modified:   01/01/2016
 * @called by:       func apiParser(rw http.ResponseWriter, req *http.Request)
 */
func templateCreate(nodes map[string]interface{}) {
	log.Println("func templateCreate()")
	params := nodes["params"].(map[string]interface{})
	templateName := params["host"].(string)
	user := "zabbix"
	groups := params["groups"]
	groupid := groups.(map[string]interface{})["groupid"].(json.Number)
	hostgroupId := string(groupid)
	now := getNow()

	o := orm.NewOrm()
	tpl := Tpl{
		Tpl_name:    templateName,
		Create_user: user,
		Create_at:   now,
	}
	log.Println("tpl =", tpl)

	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	id, err := o.Insert(&tpl)
	if err != nil {
		setError(err.Error(), result)
	} else {
		templateId := strconv.Itoa(int(id))
		templateids := [1]string{string(templateId)}
		result["templateids"] = templateids

		groupId, err := strconv.Atoi(hostgroupId)
		if err != nil {
			setError(err.Error(), result)
		}
		grp_tpl := Grp_tpl{
			Grp_id:    groupId,
			Tpl_id:    int(id),
			Bind_user: user,
		}
		log.Println("grp_tpl =", grp_tpl)

		_, err = o.Insert(&grp_tpl)
		if err != nil {
			setError(err.Error(), result)
		}
	}
	nodes["result"] = result
}

/**
 * @function name:   func templateDelete(nodes map[string]interface{})
 * @description:     This function handles template.delete API requests.
 * @related issues:  OWL-257, OWL-093, OWL-086
 * @param:           nodes map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           09/22/2015
 * @last modified:   01/01/2016
 * @called by:       func apiParser(rw http.ResponseWriter, req *http.Request)
 */
func templateDelete(nodes map[string]interface{}) {
	log.Println("func templateDelete()")
	params := nodes["params"].([]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	o := orm.NewOrm()
	args := []interface{}{}
	args = append(args, "DELETE FROM falcon_portal.tpl WHERE id=?")
	args = append(args, "DELETE FROM falcon_portal.grp_tpl WHERE tpl_id=?")
	log.Println("args =", args)

	templateids := []string{}
	for _, sqlcmd := range args {
		log.Println(sqlcmd)
		for _, templateId := range params {
			log.Println("templateId =", templateId)
			res, err := o.Raw(sqlcmd.(string), templateId).Exec()
			if err != nil {
				setError(err.Error(), result)
			} else {
				num, _ := res.RowsAffected()
				if num > 0 && sqlcmd == "DELETE FROM falcon_portal.tpl WHERE id=?" {
					templateids = append(templateids, templateId.(string))
					log.Println("delete template id =", templateId)
					log.Println("mysql row affected nums =", num)
				}
			}
		}
	}
	result["templateids"] = templateids
	nodes["result"] = result
}

func templateGet(nodes map[string]interface{}) {
	params := nodes["params"].(map[string]interface{})
	items := []interface{}{}
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	queryAll := false
	if val, ok := params["filter"]; ok {
		filter := val.(map[string]interface{})
		if val, ok = filter["name"]; ok {
			for _, keyword := range val.([]interface{}) {
				if keyword.(string) == "_all_" {
					queryAll = true
				}
			}
		}
	}
	o := orm.NewOrm()
	countOfRows := 0
	if queryAll {
		var templates []*Tpl
		num, err := o.Raw("SELECT id, tpl_name FROM falcon_portal.tpl").QueryRows(&templates)
		if err != nil {
			setError(err.Error(), result)
		} else {
			countOfRows = int(num)
			for _, template := range templates {
				item := map[string]interface{}{}
				item["templateid"] = strconv.Itoa(template.Id)
				item["name"] = template.Tpl_name
				items = append(items, item)
			}
		}
	}
	result["items"] = items
	result["count"] = countOfRows
	nodes["result"] = result
}

/**
 * @function name:   func checkTemplateExist(params map[string]interface{}, result map[string]interface{}) Host
 * @description:     This function checks if a template existed.
 * @related issues:  OWL-086
 * @param:           params map[string]interface{}
 * @param:           result map[string]interface{}
 * @return:          template Tpl
 * @author:          Don Hsieh
 * @since:           01/19/2016
 * @last modified:   01/19/2016
 * @called by:       func templateUpdate(nodes map[string]interface{})
 */
func checkTemplateExist(params map[string]interface{}, result map[string]interface{}) Tpl {
	o := orm.NewOrm()
	var template Tpl

	if val, ok := params["templateid"]; ok {
		if val != nil {
			templateId := val.(string)
			templateIdInt, err := strconv.Atoi(templateId)
			if err != nil {
				setError(err.Error(), result)
			}
			err = o.QueryTable("tpl").Filter("id", templateIdInt).One(&template)
			if err == orm.ErrMultiRows {
				// Have multiple records
				log.Println("returned multiple rows")
			} else if err == orm.ErrNoRows {
				// No result
			}
		}
	}

	if val, ok := params["name"]; ok {
		if val != nil {
			templateName := val.(string)
			err := o.QueryTable("tpl").Filter("tpl_name", templateName).One(&template)
			if err == orm.ErrMultiRows {
				// Have multiple records
				log.Println("returned multiple rows")
			} else if err == orm.ErrNoRows {
				// No result
			}
		}
	}
	return template
}

/**
 * @function name:   func bindTemplateToGroup(templateId int, params map[string]interface{}, result map[string]interface{})
 * @description:     This function binds a template to hostgroups.
 * @related issues:  OWL-086
 * @param:           templateId int
 * @param:           params map[string]interface{}
 * @param:           result map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           01/19/2016
 * @last modified:   01/19/2016
 * @called by:       func templateUpdate(nodes map[string]interface{})
 */
func bindTemplatesAndGroups(groupIds []int, templateIds []int, result map[string]interface{}) {
	o := orm.NewOrm()
	var grp_tpl Grp_tpl
	for _, groupId := range groupIds {
		for _, templateId := range templateIds {
			sqlcmd := "SELECT * FROM falcon_portal.grp_tpl WHERE grp_id=? AND tpl_id=?"
			err := o.Raw(sqlcmd, groupId, templateId).QueryRow(&grp_tpl)
			if err == orm.ErrNoRows {
				grp_tpl := Grp_tpl{
					Grp_id:    groupId,
					Tpl_id:    templateId,
					Bind_user: "zabbix",
				}
				log.Println("grp_tpl =", grp_tpl)
				_, err = o.Insert(&grp_tpl)
				if err != nil {
					setError(err.Error(), result)
				}
			} else if err != nil {
				setError(err.Error(), result)
			} else {
				log.Println("grp_tpl existed =", grp_tpl)
			}
		}
	}
}

func unbindTemplateAndGroups(templateId string, result map[string]interface{}) {
	o := orm.NewOrm()
	sql := "DELETE FROM grp_tpl WHERE tpl_id = ?"
	res, err := o.Raw(sql, templateId).Exec()
	if err != nil {
		setError(err.Error(), result)
	}
	num, _ := res.RowsAffected()
	log.Println("mysql row affected nums =", num)
}

/**
 * @function name:   func templateUpdate(nodes map[string]interface{})
 * @description:     This function updates template data.
 * @related issues:  OWL-257, OWL-093, OWL-086
 * @param:           nodes map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           09/22/2015
 * @last modified:   01/19/2016
 * @called by:       func apiParser(rw http.ResponseWriter, req *http.Request)
 */
func templateUpdate(nodes map[string]interface{}) {
	params := nodes["params"].(map[string]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	template := checkTemplateExist(params, result)
	if template.Id > 0 {
		groupIds := []int{}
		templateIds := []int{}
		groups := params["groups"].([]interface{})
		for _, group := range groups {
			groupId := group.(map[string]interface{})["groupid"].(string)
			groupIdInt, err := strconv.Atoi(groupId)
			log.Println("groupIdInt =", groupIdInt)
			if err != nil {
				setError(err.Error(), result)
			}
			groupIds = append(groupIds, groupIdInt)
		}
		templateIds = append(templateIds, template.Id)
		templateid := strconv.Itoa(template.Id)
		unbindTemplateAndGroups(templateid, result)
		bindTemplatesAndGroups(groupIds, templateIds, result)
		templateids := [1]string{string(templateid)}
		result["templateids"] = templateids
	} else {
		log.Println("template not existed")
	}
	nodes["result"] = result
}

/**
 * @function name:   func getFctoken() fctoken string
 * @description:     This function returns fctoken for API request.
 * @related issues:  OWL-159
 * @param:           void
 * @return:          fctoken string
 * @author:          Don Hsieh
 * @since:           11/24/2015
 * @last modified:   11/24/2015
 * @called by:       func apiAlert(rw http.ResponseWriter, req *http.Request)
 *                    in query/http/zabbix.go
 *                   func getMapValues(chartType string) map[string]interface{}
 *                    in query/http/grafana.go
 */
func getFctoken() string {
	hasher := md5.New()
	io.WriteString(hasher, g.Config().Api.Token)
	s := hex.EncodeToString(hasher.Sum(nil))

	t := time.Now()
	now := t.Format("20060102")
	s = now + s

	hasher = md5.New()
	io.WriteString(hasher, s)
	fctoken := hex.EncodeToString(hasher.Sum(nil))
	return fctoken
}

/**
 * @function name:   func apiAlert(rw http.ResponseWriter, req *http.Request)
 * @description:     This function handles alarm API request.
 * @related issues:  OWL-159, OWL-093
 * @param:           rw http.ResponseWriter
 * @param:           req *http.Request
 * @return:          void
 * @author:          Don Hsieh
 * @since:           09/29/2015
 * @last modified:   11/24/2015
 * @called by:       func apiParser(rw http.ResponseWriter, req *http.Request)
 */
func apiAlert(rw http.ResponseWriter, req *http.Request) {
	fcname := g.Config().Api.Name
	fctoken := getFctoken()
	param := req.URL.Query()
	log.Println("param =", param)
	arr := param["endpoint"]
	hostname := arr[0]
	arr = param["time"]
	datetime := arr[0]

	arr = param["stra_id"]
	trigger_id, err := strconv.Atoi(arr[0])
	if err != nil {
		log.Println(err.Error())
	}
	arr = param["metric"]
	metric := arr[0]
	arr = param["step"]
	step := arr[0]
	arr = param["tpl_id"]
	tpl_id := arr[0]
	arr = param["status"]
	zabbix_status := arr[0]
	arr = param["priority"]
	zabbix_level := arr[0]
	summary := "[OWL] " + metric + "_" + step + "_" + zabbix_level

	args := map[string]interface{}{
		"summary":       summary,
		"zabbix_status": zabbix_status, // "PROBLEM",
		"zabbix_level":  "Information", // "Information" or "High"
		"trigger_id":    trigger_id,
		"host_ip":       "",
		"hostname":      hostname,
		"event_id":      tpl_id,
		"template_name": "Template Server Basic Monitor",
		"datetime":      datetime,
		"fcname":        fcname,
		"fctoken":       fctoken,
	}

	log.Println("args =", args)
	bs, err := json.Marshal(args)
	if err != nil {
		log.Println("Error =", err.Error())
	}

	url := g.Config().Api.Event
	log.Println("url =", url)

	reqAlert, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(bs)))
	if err != nil {
		log.Println("Error =", err.Error())
	}
	reqAlert.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(reqAlert)
	if err != nil {
		log.Println("Error =", err.Error())
	}
	defer resp.Body.Close()

	log.Println("response Status =", resp.Status) // 200 OK   TypeOf(resp.Status): string
	log.Println("response Headers =", resp.Header)
	body, _ := ioutil.ReadAll(resp.Body)
	log.Println("response Body =", string(body))
	rw.Header().Set("Content-Type", "application/json; charset=UTF-8")
	rw.Write(body)
}

/**
 * @function name:   func setResponse(rw http.ResponseWriter, resp map[string]interface{})
 * @description:     This function sets content of response and returns it.
 * @related issues:  OWL-283, OWL-257
 * @param:           rw http.ResponseWriter
 * @param:           resp map[string]interface{}
 * @return:          void
 * @author:          Don Hsieh
 * @since:           01/01/2016
 * @last modified:   01/14/2016
 * @called by:       func apiParser(rw http.ResponseWriter, req *http.Request)
 */
func setResponse(rw http.ResponseWriter, resp map[string]interface{}) {
	if _, ok := resp["auth"]; ok {
		delete(resp, "auth")
	}
	if _, ok := resp["method"]; ok {
		delete(resp, "method")
	}
	if _, ok := resp["params"]; ok {
		delete(resp, "params")
	}
	result := resp["result"].(map[string]interface{})
	if val, ok := result["error"]; ok {
		errors := val.([]string)
		if len(errors) > 0 {
			delete(resp, "result")
			resp["error"] = errors
		} else {
			delete(resp["result"].(map[string]interface{}), "error")
			if val, ok = result["items"]; ok {
				resp["result"] = val
			}
			if val, ok = result["count"]; ok {
				resp["count"] = val
			}
			if val, ok = result["anomalies"]; ok {
				resp["anomalies"] = val
			}
		}
	}
	resp["time"] = getNow()
	RenderJson(rw, resp)
}

/**
 * @function name:   func apiParser(rw http.ResponseWriter, req *http.Request)
 * @description:     This function parses the method of API request.
 * @related issues:  OWL-257, OWL-254, OWL-085
 * @param:           rw http.ResponseWriter
 * @param:           req *http.Request
 * @return:          void
 * @author:          Don Hsieh
 * @since:           09/11/2015
 * @last modified:   01/01/2016
 * @called by:       http.HandleFunc("/api", apiParser)
 *                    in func main()
 */
func apiParser(rw http.ResponseWriter, req *http.Request) {
	log.Println("func apiParser(rw http.ResponseWriter, req *http.Request)")
	buf := new(bytes.Buffer)
	buf.ReadFrom(req.Body)
	log.Println(buf.Len())
	if buf.Len() == 0 {
		apiAlert(rw, req)
	} else {
		s := buf.String() // Does a complete copy of the bytes in the buffer.
		log.Println("s =", s)
		json, err := simplejson.NewJson(buf.Bytes())
		if err != nil {
			log.Println(err.Error())
		}

		var nodes = make(map[string]interface{})
		nodes, _ = json.Map()
		method := nodes["method"]
		log.Println(method)

		if method == "host.create" {
			hostCreate(nodes)
		} else if method == "host.delete" {
			hostDelete(nodes)
		} else if method == "host.get" {
			hostGet(nodes)
		} else if method == "host.update" {
			hostUpdate(nodes)
		} else if method == "hostgroup.create" {
			hostgroupCreate(nodes)
		} else if method == "hostgroup.delete" {
			hostgroupDelete(nodes)
		} else if method == "hostgroup.get" {
			hostgroupGet(nodes)
		} else if method == "hostgroup.update" {
			hostgroupUpdate(nodes)
		} else if method == "template.create" {
			templateCreate(nodes)
		} else if method == "template.delete" {
			templateDelete(nodes)
		} else if method == "template.get" {
			templateGet(nodes)
		} else if method == "template.update" {
			templateUpdate(nodes)
		}
		setResponse(rw, nodes)
	}
}

/**
 * @function name:   func configZabbixRoutes()
 * @description:     This function handles API requests.
 * @related issues:  OWL-093, OWL-085
 * @param:           void
 * @return:          void
 * @author:          Don Hsieh
 * @since:           09/09/2015
 * @last modified:   10/21/2015
 * @called by:       func Start()
 *                    in http/http.go
 */
func configZabbixRoutes() {
	http.HandleFunc("/api", apiParser)
}
