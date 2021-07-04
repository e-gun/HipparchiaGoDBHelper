//    HipparchiaGoDBHelper: search and vector helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2021
//    License: GNU GENERAL PUBLIC LICENSE 3
//        (see LICENSE in the top level directory of the distribution)

//	this is supposed to be very basic
//	[a] it launches and starts listening on a port
//	[b] it waits to receive a websocket message: this is a search key ID (e.g., '2f81c630')
//	[c] it then looks inside of redis for the relevant polling data associated with that search ID
//	[d] it parses, packages (as JSON), and then redistributes this information back over the websocket
//	[e] when the poll disappears from redis, the messages stop broadcasting

package main

import (
	"C"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/olahol/melody.v1"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// HipparchiaWebsocket : fire up our own websocket server because wscheckpoll() in HipparchiaServer is unavailable to golang users
// note that HipparchiaServer can't call this as a module at helperappwebsocketserver() without blocking. So there is no interface for that
func HipparchiaWebsocket() {
	port := cfg.WSPort

	msg(fmt.Sprintf("WebSockets Launched"), 1)
	if cfg.LogLevel < 2 {
		gin.SetMode(gin.ReleaseMode)
	}

	ws := gin.New()
	m := melody.New()

	ws.GET("/", func(c *gin.Context) {
		e := m.HandleRequest(c.Writer, c.Request)
		if e != nil {
			fmt.Println("melody choked when it tried to 'HandleRequest'")
		}
	})

	m.HandleMessage(func(s *melody.Session, searchid []byte) {
		if len(searchid) < 16 {
			// at this point you have "ebf24e19" and NOT ebf24e19; fix that
			// id := string(searchid[1 : len(searchid)-1])
			keycleaner := regexp.MustCompile(`[^a-f0-9]`)
			id := keycleaner.ReplaceAllString(string(searchid), "")
			msg(fmt.Sprintf("id is %s", id), 1)
			runpollmessageloop(id, m)
		}
	})

	err := ws.Run(fmt.Sprintf(":%d", port))
	checkerror(err)
}

func runpollmessageloop(searchid string, m *melody.Melody) {
	failthreshold := cfg.WSFail
	saving := cfg.WSSave

	rc := grabredisconnection()
	defer func(rc redis.Conn) {
		err := rc.Close()
		checkerror(err)
	}(rc)

	// note that these are lower case inside of redis, but they get reported as upper-case
	// this all comes back to problems exporting fields of structs and our use of reflect
	var rediskeys [8]string
	rediskeys[0] = "launchtime"
	rediskeys[1] = "active"
	rediskeys[2] = "statusmessage"
	rediskeys[3] = "remaining"
	rediskeys[4] = "poolofwork"
	rediskeys[5] = "hitcount"
	rediskeys[6] = "portnumber"
	rediskeys[7] = "notes"

	missing := 0
	iterations := 0
	for {
		iterations += 1
		msg(fmt.Sprintf("WebSocket server reports that runpollmessageloop() for %s is on iteration #%d", searchid, iterations), 1)
		time.Sleep(pollinginterval)
		// the flow is a bit fussy, but separation should allow for easier maintenance if/when things
		// change on HipparchiaServer's end
		redisvals := retrievepollingdata(searchid, rediskeys, rc)
		cpd := typeconvertpollingdata(searchid, rediskeys, redisvals)
		jsonreply, err := json.Marshal(cpd)
		checkerror(err)
		e := m.Broadcast(jsonreply)
		checkerror(e)

		// rediskeys[1] = "active"
		if redisvals[1] == "" {
			// poll does not exist yet or never existed
			missing += 1
			msg(fmt.Sprintf("%s_%s = %s ; missing is now %d",
				searchid, rediskeys[1], redisvals[1], missing), 1)
		}
		if redisvals[1] == "no" {
			missing += 1
			msg(fmt.Sprintf("%s_%s = %s ; missing is now %d",
				searchid, rediskeys[1], redisvals[1], missing), 1)
		}
		if missing >= failthreshold {
			msg(fmt.Sprintf("breaking for %s because missing >= failthreshold: %d >= %d",
				searchid, missing, failthreshold), 2)
			break
		}
	}
	if saving < 1 {
		deletewhendone(searchid, rediskeys, rc)
	} else {
		msg(fmt.Sprintf("retained redis keys for %s", searchid), 1)
	}
}

func retrievepollingdata(searchid string, rediskeys [8]string, rc redis.Conn) [8]string {
	// grab the data from redis
	var redisvals [8]string
	var err error
	for i := 0; i < len(rediskeys); i++ {
		k := fmt.Sprintf("%s_%s", searchid, rediskeys[i])
		redisvals[i], err = redis.String(rc.Do("GET", k))
		if err != nil {
			// checkerror(err) will yield "panic: redis: nil"
			redisvals[i] = ""
		}
		msg(fmt.Sprintf("%s_%s = %s", searchid, rediskeys[i], redisvals[i]), 3)
	}
	return redisvals
}

func typeconvertpollingdata(searchid string, rediskeys [8]string, redisvals [8]string) CompositePollingData {
	// everything arrives as a string; but that is not technically right
	// *but* since you are going back to JSON, you can in practice skip getting the types right inside of golang
	// the rust version just grabs the data; hashmaps it with the right keys; then jsonifies it
	// the equivalent here would be to just let CompositePollingData have String as its type in every field, i.e.,
	// all of the reflect tests could be skipped; nevertheless it is useful to have a sample of how to implement reflect...

	// https://stackoverflow.com/questions/6395076/using-reflect-how-do-you-set-the-value-of-a-struct-field/6396678#6396678
	// https://samwize.com/2015/03/20/how-to-use-reflect-to-set-a-struct-field/

	var cpd CompositePollingData

	// attempt to convert to the proper type:
	// at any given index:
	//	[a] determine the kind of the data required for this value
	//	[b] convert the string we have stared at redisvals into the proper type
	//	[c] store the converted data in the right field inside of cpd

	cpd.ID = searchid
	for i := 0; i < len(redisvals); i++ {
		// sadly we have to capitalize the fields to export them and this means they do not match the source
		n := strings.Title(rediskeys[i])
		k := reflect.ValueOf(&cpd).Elem().FieldByName(n).Kind()
		switch k {
		case reflect.Float64:
			v, err := strconv.ParseFloat(redisvals[i], 64)
			if err != nil {
				v = 0.0
			}
			reflect.ValueOf(&cpd).Elem().FieldByName(n).SetFloat(v)
		case reflect.String:
			v := redisvals[i]
			reflect.ValueOf(&cpd).Elem().FieldByName(n).SetString(v)
		case reflect.Int:
			v, err := strconv.Atoi(redisvals[i])
			if err != nil {
				v = 0
			}
			reflect.ValueOf(&cpd).Elem().FieldByName(n).SetInt(int64(v))
		case reflect.Int64:
			v, err := strconv.Atoi(redisvals[i])
			if err != nil {
				v = 0
			}
			reflect.ValueOf(&cpd).Elem().FieldByName(n).SetInt(int64(v))
		case reflect.Bool:
			// not actually used, but...
			v, err := strconv.ParseBool(redisvals[i])
			if err != nil {
				v = false
			}
			reflect.ValueOf(&cpd).Elem().FieldByName(n).SetBool(v)
		}
	}
	return cpd
}

func deletewhendone(searchid string, rediskeys [8]string, rc redis.Conn) {
	// make sure that the "there is no work" message gets propagated
	p := fmt.Sprintf("%s_poolofwork", searchid)
	_, _ = rc.Do("SET", p, -1)
	time.Sleep(pollinginterval)
	// get rid of the polling keys
	for i := 0; i < len(rediskeys); i++ {
		k := fmt.Sprintf("%s_%s", searchid, rediskeys[i])
		_, _ = rc.Do("DEL", k)
	}
	msg(fmt.Sprintf("deleted redis keys for %s", searchid), 2)
}
