//    HipparchiaGoDBHelper: search and vector helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2016-21
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
	"github.com/go-redis/redis"
	"gopkg.in/olahol/melody.v1"
	"reflect"
	"strconv"
	"strings"
	"time"
)

//fire up our own websocket server because wscheckpoll() in HipparchiaServer is unavailable to golang module users
func StartHipparchiaPollWebsocket(port int, loglevel int, failthreshold int, saving int, rl RedisLogin) {
	logiflogging(fmt.Sprintf("WebSocket Module Launched"), loglevel, 1)
	if loglevel < 2 {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	m := melody.New()

	r.GET("/", func(c *gin.Context) {
		e := m.HandleRequest(c.Writer, c.Request)
		if e != nil {
			fmt.Println("melody choked when it tried to 'HandleRequest'")
		}
	})

	m.HandleMessage(func(s *melody.Session, searchid []byte) {
		// at this point you have "ebf24e19" and NOT ebf24e19; fix that
		id := string(searchid[1 : len(searchid)-1])
		logiflogging(fmt.Sprintf("id is %s", id), loglevel, 1)
		runpollmessageloop(id, loglevel, failthreshold, saving, rl, m)
	})

	err := r.Run(fmt.Sprintf(":%d", port))
	checkerror(err)
}

func runpollmessageloop(searchid string, loglevel int, failthreshold int, saving int, rl RedisLogin, m *melody.Melody) {
	redisclient := redis.NewClient(&redis.Options{Addr: rl.Addr, Password: rl.Password, DB: rl.DB})
	defer redisclient.Close()

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
		logiflogging(fmt.Sprintf("%s - runpollmessageloop() is on iteration #%d", searchid, iterations), loglevel, 1)
		time.Sleep(pollinginterval)
		// the flow is a bit fussy, but separation should allow for easier maintenance if/when things
		// change on HipparchiaServer's end
		redisvals := retrievepollingdata(searchid, rediskeys, loglevel, redisclient)
		cpd := typeconvertpollingdata(rediskeys, redisvals)
		jsonreply, err := json.Marshal(cpd)
		checkerror(err)
		e := m.Broadcast(jsonreply)
		checkerror(e)

		// rediskeys[1] = "active"
		if redisvals[1] == "" {
			// poll does not exist yet or never existed
			missing += 1
			logiflogging(fmt.Sprintf("%s_%s = %s ; missing is now %d",
				searchid, rediskeys[1], redisvals[1], missing), loglevel, 1)
		}
		if redisvals[1] == "no" {
			missing += 1
			logiflogging(fmt.Sprintf("%s_%s = %s ; missing is now %d",
				searchid, rediskeys[1], redisvals[1], missing), loglevel, 1)
		}
		if missing >= failthreshold {
			logiflogging(fmt.Sprintf("breaking for %s because missing >= failthreshold: %d >= %d",
				searchid, missing, failthreshold), loglevel, 2)
			break
		}
	}
	if saving < 1 {
		deletewhendone(searchid, rediskeys, loglevel, redisclient)
	} else {
		logiflogging(fmt.Sprintf("retained redis keys for %s", searchid), loglevel, 1)
	}
}

func retrievepollingdata(searchid string, rediskeys [8]string, loglevel int, rc *redis.Client) [8]string {
	// grab the data from redis
	var redisvals [8]string
	var err error
	for i := 0; i < len(rediskeys); i++ {
		redisvals[i], err = rc.Get(fmt.Sprintf("%s_%s", searchid, rediskeys[i])).Result()
		if err != nil {
			// checkerror(err) will yield "panic: redis: nil"
			redisvals[i] = ""
		}
		logiflogging(fmt.Sprintf("%s_%s = %s", searchid, rediskeys[i], redisvals[i]), loglevel, 3)
	}
	return redisvals
}

func typeconvertpollingdata(rediskeys [8]string, redisvals [8]string) CompositePollingData {
	// everything arrives as a string; but that is not right
	// https://stackoverflow.com/questions/6395076/using-reflect-how-do-you-set-the-value-of-a-struct-field/6396678#6396678
	// https://samwize.com/2015/03/20/how-to-use-reflect-to-set-a-struct-field/

	var cpd CompositePollingData

	// attempt to convert to the proper type:
	// at any given index:
	//	[a] determine the kind of the data required for this value
	//	[b] convert the string we have stared at redisvals into the proper type
	//	[c] store the converted data in the right field inside of cpd

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
		}
	}
	return cpd
}

func deletewhendone(searchid string, rediskeys [8]string, loglevel int, rc *redis.Client) {
	// get rid of the polling keys
	for i := 0; i < len(rediskeys); i++ {
		_, _ = rc.Del(fmt.Sprintf("%s_%s", searchid, rediskeys[i])).Result()
	}
	logiflogging(fmt.Sprintf("deleted redis keys for %s", searchid), loglevel, 1)
}
