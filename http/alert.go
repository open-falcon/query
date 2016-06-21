package http

import (
	"fmt"
	"github.com/Cepave/query/logger"
	"github.com/astaxie/beego/orm"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

func parsePlatformJSON(result map[string]interface{}) map[string]interface{} {
	var nodes = make(map[string]interface{})
	getPlatformJSON(nodes, result)
	hosts := map[string]interface{}{}
	hostnames := []string{}
	hostnamesMap := map[string]int{}
	if int(nodes["status"].(float64)) == 1 {
		hostname := ""
		for _, platform := range nodes["result"].([]interface{}) {
			platformName := platform.(map[string]interface{})["platform"].(string)
			for _, device := range platform.(map[string]interface{})["ip_list"].([]interface{}) {
				hostname = device.(map[string]interface{})["hostname"].(string)
				if _, ok := hostnamesMap[hostname]; !ok {
					ip := device.(map[string]interface{})["ip"].(string)
					if ip == getIPFromHostname(hostname, result) {
						hostnames = append(hostnames, hostname)
						idcID := device.(map[string]interface{})["pop_id"].(string)
						host := map[string]interface{}{
							"activate": device.(map[string]interface{})["ip_status"].(string),
							"hostname": hostname,
							"idcID":    idcID,
							"ip":       ip,
							"platform": platformName,
						}
						hostnamesMap[hostname] = 1
						hosts[hostname] = host
					}
				} else {
					host := hosts[hostname].(map[string]interface{})
					platforms := strings.Split(host["platform"].(string), ", ")
					platforms = appendUniqueString(platforms, platformName)
					host["platform"] = strings.Join(platforms, ", ")
					hosts[hostname] = host
				}
			}
		}
		sort.Strings(hostnames)
		idcIDsMap := map[string]string{}
		idcNames := []string{}
		o := orm.NewOrm()
		var idcs []*Idc
		sqlcommand := "SELECT pop_id, name FROM grafana.idc ORDER BY pop_id ASC"
		_, err := o.Raw(sqlcommand).QueryRows(&idcs)
		if err != nil {
			setError(err.Error(), result)
		} else {
			for _, idc := range idcs {
				idcIDsMap[strconv.Itoa(idc.Pop_id)] = idc.Name
				idcNames = appendUniqueString(idcNames, idc.Name)
			}
		}
		sort.Strings(idcNames)
		for _, hostname := range hostnames {
			host := hosts[hostname].(map[string]interface{})
			idcID := host["idcID"].(string)
			if _, ok := idcIDsMap[idcID]; ok {
				idcName := idcIDsMap[idcID]
				host["idc"] = idcName
				delete(host, "idcID")
			}
		}
	}
	if _, ok := nodes["info"]; ok {
		delete(nodes, "info")
	}
	if _, ok := nodes["status"]; ok {
		delete(nodes, "status")
	}
	return hosts
}

func getSeverity(priority string) string {
	severity := "Lower"
	if priority == "0" {
		severity = "High"
	} else if priority == "1" {
		severity = "Medium"
	} else if priority == "2" || priority == "3" {
		severity = "Low"
	}
	return severity
}

func getStatus(statusRaw string) string {
	status := ""
	if statusRaw == "PROBLEM" {
		status = "Triggered"
	} else if statusRaw == "OK" {
		status = "Recovered"
	}
	return status
}

func getProcess(status string) string {
	process := ""
	if status == "Recovered" {
		process = "Resolved"
	} else if status == "Triggered" {
		process = "Unresolved"
	}
	return process
}

func getDuration(timeTriggered string, result map[string]interface{}) string {
	date, err := time.Parse("2006-01-02 15:04", timeTriggered)
	if err != nil {
		setError(err.Error(), result)
	}
	now := time.Now().Unix()
	diff := now - date.Unix()
	if diff <= 60 {
		return "just now"
	}
	if diff <= 120 {
		return "1 minute ago"
	}
	if diff <= 3600 {
		return fmt.Sprintf("%d minutes ago", diff/60)
	}
	if diff <= 7200 {
		return "1 hour ago"
	}
	if diff <= 3600*24 {
		return fmt.Sprintf("%d hours ago", diff/3600)
	}
	if diff <= 3600*24*2 {
		return "1 day ago"
	}
	return fmt.Sprintf("%d days ago", diff/3600/24)
}

func getUserID(username string, result map[string]interface{}) (string, string) {
	userID := ""
	userRole := ""
	o := orm.NewOrm()
	var rows []orm.Params
	sqlcmd := "SELECT id, role FROM uic.user WHERE name = ? LIMIT 1"
	num, err := o.Raw(sqlcmd, username).Values(&rows)
	if err != nil {
		setError(err.Error(), result)
	} else if num > 0 {
		for _, row := range rows {
			userID = row["id"].(string)
			userRole = row["role"].(string)
		}
	}
	return userID, userRole
}

func checkLoggedIn(userID string, sig string, result map[string]interface{}) bool {
	isLoggedIn := false
	o := orm.NewOrm()
	var rows []orm.Params
	expired := ""
	sqlcmd := "SELECT expired FROM uic.session WHERE uid = ? AND sig = ? LIMIT 1"
	num, err := o.Raw(sqlcmd, userID, sig).Values(&rows)
	if err != nil {
		setError(err.Error(), result)
	} else if num > 0 {
		for _, row := range rows {
			expired = row["expired"].(string)
		}
	}
	expiredInt, err := strconv.Atoi(expired)
	if err != nil {
		setError(err.Error(), result)
	} else {
		now := time.Now().Unix()
		isLoggedIn = int64(expiredInt) > now
	}
	return isLoggedIn
}

func getReceiverTeamIDs(userID string, result map[string]interface{}) []string {
	receiverTeamIDs := []string{}
	o := orm.NewOrm()
	var rows []orm.Params
	sqlcmd := "SELECT tid FROM uic.rel_team_user WHERE uid = ?"
	num, err := o.Raw(sqlcmd, userID).Values(&rows)
	if err != nil {
		setError(err.Error(), result)
	} else if num > 0 {
		for _, row := range rows {
			receiverTeamID := row["tid"].(string)
			receiverTeamIDs = append(receiverTeamIDs, receiverTeamID)
		}
	}
	return receiverTeamIDs
}

func getReceiverTeamNames(receiverTeamIDs []string, result map[string]interface{}) []string {
	receiverTeamNames := []string{}
	o := orm.NewOrm()
	var rows []orm.Params
	sqlcmd := "SELECT name FROM uic.team WHERE id IN ("
	sqlcmd += strings.Join(receiverTeamIDs, ",") + ")"
	num, err := o.Raw(sqlcmd).Values(&rows)
	if err != nil {
		setError(err.Error(), result)
	} else if num > 0 {
		for _, row := range rows {
			receiverTeamName := row["name"].(string)
			receiverTeamNames = append(receiverTeamNames, receiverTeamName)
		}
	}
	return receiverTeamNames
}

func getActionIDs(receiverTeamNames []string, result map[string]interface{}) []string {
	actionIDs := []string{}
	o := orm.NewOrm()
	var rows []orm.Params
	sqlcmd := "SELECT id FROM falcon_portal.action WHERE uic IN ('"
	sqlcmd += strings.Join(receiverTeamNames, "','") + "')"
	num, err := o.Raw(sqlcmd).Values(&rows)
	if err != nil {
		setError(err.Error(), result)
	} else if num > 0 {
		for _, row := range rows {
			actionID := row["id"].(string)
			actionIDs = append(actionIDs, actionID)
		}
	}
	return actionIDs
}

func getTemplateIDs(username string, sig string, result map[string]interface{}) string {
	templateIDs := []string{}
	userID, userRole := getUserID(username, result)
	if userID == "" {
		setError("User not found", result)
		return ""
	}
	isLoggedIn := checkLoggedIn(userID, sig, result)
	if !isLoggedIn {
		setError("Please log in first", result)
		return ""
	}
	if userRole == "2" || userRole == "1" { // admin user
		return "*"
	}
	receiverTeamIDs := getReceiverTeamIDs(userID, result)
	if len(receiverTeamIDs) == 0 {
		setError("User not subscribe any alerts", result)
		return ""
	}
	receiverTeamNames := getReceiverTeamNames(receiverTeamIDs, result)
	if len(receiverTeamNames) == 0 {
		setError("User not subscribe any alerts", result)
		return ""
	}
	actionIDs := getActionIDs(receiverTeamNames, result)
	if len(actionIDs) == 0 {
		setError("User not subscribe any alerts", result)
		return ""
	}
	o := orm.NewOrm()
	var rows []orm.Params
	sqlcmd := "SELECT id FROM falcon_portal.tpl WHERE action_id IN ("
	sqlcmd += strings.Join(actionIDs, ",") + ")"
	num, err := o.Raw(sqlcmd).Values(&rows)
	if err != nil {
		setError(err.Error(), result)
	} else if num > 0 {
		for _, row := range rows {
			templateID := row["id"].(string)
			templateIDs = append(templateIDs, templateID)
		}
	}
	return strings.Join(templateIDs, ",")
}

func getUsers(result map[string]interface{}) map[string]string {
	users := map[string]string{}
	o := orm.NewOrm()
	var rows []orm.Params
	sqlcmd := "SELECT id, name FROM uic.user"
	num, err := o.Raw(sqlcmd).Values(&rows)
	if err != nil {
		setError(err.Error(), result)
	} else if num > 0 {
		for _, row := range rows {
			userID := row["id"].(string)
			users[userID] = row["name"].(string)
		}
	}
	return users
}

func getNotes(result map[string]interface{}) map[string]interface{} {
	notes := map[string]interface{}{}
	users := getUsers(result)
	o := orm.NewOrm()
	var rows []orm.Params
	sqlcmd := "SELECT event_caseId, note, status, timestamp, user_id "
	sqlcmd += "FROM falcon_portal.event_note ORDER BY timestamp DESC"
	num, err := o.Raw(sqlcmd).Values(&rows)
	if err != nil {
		setError(err.Error(), result)
	} else if num > 0 {
		for _, row := range rows {
			hash := row["event_caseId"].(string)
			time := row["timestamp"].(string)
			time = time[:len(time)-3]
			userID := row["user_id"].(string)
			note := map[string]string{
				"note":   row["note"].(string),
				"status": row["status"].(string),
				"user":   users[userID],
				"time":   time,
			}
			if slice, ok := notes[hash]; ok {
				slice = append(slice.([]map[string]string), note)
				notes[hash] = slice
			} else {
				notes[hash] = []map[string]string{
					note,
				}
			}
		}
	}
	return notes
}

func queryAlerts(templateIDs string, result map[string]interface{}) []interface{} {
	mylog := logger.Logger()
	alerts := []interface{}{}
	if templateIDs == "" {
		return alerts
	}
	notes := getNotes(result)
	o := orm.NewOrm()
	var rows []orm.Params
	sqlcmd := "SELECT id, endpoint, metric, note, priority, status, timestamp, template_id, tpl_creator, process_status "
	sqlcmd += "FROM falcon_portal.event_cases "
	if templateIDs != "*" {
		sqlcmd += "WHERE template_id IN ("
		sqlcmd += templateIDs + ") "
	}
	sqlcmd += "ORDER BY timestamp DESC LIMIT 600"
	num, err := o.Raw(sqlcmd).Values(&rows)
	if err != nil {
		setError(err.Error(), result)
	} else if num > 0 {
		for _, row := range rows {
			hash := row["id"].(string)
			hostname := row["endpoint"].(string)
			metric := row["metric"].(string)
			metricType := strings.Split(metric, ".")[0]
			content := row["note"].(string)
			priority := row["priority"].(string)
			statusRaw := row["status"].(string)
			time := row["timestamp"].(string)
			time = time[:len(time)-3]
			process := getProcess(getStatus(statusRaw))
			note := []map[string]string{}
			if _, ok := notes[hash]; ok {
				note = notes[hash].([]map[string]string)
				process = row["process_status"].(string)
				mylog.Debug(fmt.Sprintf("process: %v", process))
				process = strings.Replace(process, process[:1], strings.ToUpper(process[:1]), 1)
			}
			templateID := row["template_id"].(string)
			author := row["tpl_creator"].(string)
			alert := map[string]interface{}{
				"hash":       hash,
				"hostname":   hostname,
				"metric":     metric,
				"author":     author,
				"templateID": templateID,
				"priority":   priority,
				"severity":   getSeverity(priority),
				"status":     getStatus(statusRaw),
				"statusRaw":  statusRaw,
				"type":       metricType,
				"content":    content,
				"time":       time,
				"duration":   getDuration(time, result),
				"note":       note,
				"process":    process,
			}
			alerts = append(alerts, alert)
		}
	}
	return alerts
}

func addPlatformToAlerts(alerts []interface{}, result map[string]interface{}, nodes map[string]interface{}, rw http.ResponseWriter) ([]interface{}, map[string]string) {
	items := []interface{}{}
	hostnames := []string{}
	platformNames := []string{}
	hostsMap := parsePlatformJSON(result)
	for _, item := range alerts {
		hostname := item.(map[string]interface{})["hostname"].(string)
		if _, ok := hostsMap[hostname]; ok {
			host := hostsMap[hostname].(map[string]interface{})
			activate := host["activate"].(string)
			if activate == "1" {
				item.(map[string]interface{})["ip"] = host["ip"].(string)
				item.(map[string]interface{})["platform"] = host["platform"].(string)
				item.(map[string]interface{})["idc"] = host["idc"].(string)
				contact := map[string]string{
					"email": "",
					"name":  "",
					"phone": "",
				}
				contacts := []interface{}{
					contact,
				}
				item.(map[string]interface{})["contact"] = contacts
				items = append(items, item)
			} else {
				log.Println("host deactivated:", hostname)
			}
		} else {
			item.(map[string]interface{})["ip"] = "-"
			item.(map[string]interface{})["platform"] = "-"
			item.(map[string]interface{})["idc"] = "-"
			contact := map[string]string{
				"email": "-",
				"name":  "-",
				"phone": "-",
			}
			contacts := []interface{}{
				contact,
			}
			item.(map[string]interface{})["contact"] = contacts
			items = append(items, item)
			log.Println("hostname not found:", hostname)
		}
	}
	for _, item := range items {
		hostname := item.(map[string]interface{})["hostname"].(string)
		hostnames = appendUniqueString(hostnames, hostname)
	}
	sort.Strings(hostnames)

	hostsTriggeredMap := map[string]string{}
	for _, hostname := range hostnames {
		if _, ok := hostsMap[hostname]; ok {
			host := hostsMap[hostname].(map[string]interface{})
			platformName := host["platform"].(string)
			if strings.Index(platformName, ", ") > -1 {
				for _, name := range strings.Split(platformName, ", ") {
					platformNames = appendUniqueString(platformNames, name)
				}
			} else {
				platformNames = appendUniqueString(platformNames, platformName)
			}
			hostsTriggeredMap[hostname] = platformName
		}
	}
	sort.Strings(platformNames)
	getPlatformContact(strings.Join(platformNames, ","), rw, nodes)
	platforms := nodes["result"].(map[string]interface{})["items"].(map[string]interface{})
	if len(platforms) > 0 {
		for _, item := range items {
			hostname := item.(map[string]interface{})["hostname"].(string)
			if _, ok := hostsMap[hostname]; ok {
				host := hostsMap[hostname].(map[string]interface{})
				platformName := host["platform"].(string)
				if strings.Index(platformName, ", ") > -1 {
					platformName = strings.Split(platformName, ", ")[0]
				}
				if contact, ok := platforms[platformName]; ok {
					item.(map[string]interface{})["contact"] = contact
				} else {
					item.(map[string]interface{})["contact"] = "BOSS 沒有平台負責人資訊"
				}
			}
		}
	}
	return items, hostsTriggeredMap
}

func getAlertCount(items []interface{}) map[string]int {
	count := map[string]int{
		"all":    len(items),
		"high":   0,
		"medium": 0,
		"low":    0,
		"lower":  0,
	}
	for _, item := range items {
		severity := item.(map[string]interface{})["severity"].(string)
		if severity == "High" {
			count["high"]++
		} else if severity == "Medium" {
			count["medium"]++
		} else if severity == "Low" {
			count["low"]++
		} else if severity == "Lower" {
			count["lower"]++
		}
	}
	return count
}

func getAlerts(rw http.ResponseWriter, req *http.Request) {
	var nodes = make(map[string]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	alerts := []interface{}{}
	items := []interface{}{}
	username := req.URL.Query()["user"][0]
	sig := req.URL.Query()["sig"][0]
	templateIDs := getTemplateIDs(username, sig, result)
	alerts = queryAlerts(templateIDs, result)
	items, hostToPlatform := addPlatformToAlerts(alerts, result, nodes, rw)
	count := getAlertCount(items)
	result["items"] = items
	nodes["result"] = result
	nodes["count"] = count
	nodes["hosts"] = hostToPlatform
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	setResponse(rw, nodes)
}

func configAlertRoutes() {
	http.HandleFunc("/api/alerts", getAlerts)
}
