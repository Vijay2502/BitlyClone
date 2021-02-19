package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"io/ioutil"
	"database/sql"
	"github.com/codegangsta/negroni"
	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"encoding/json"
	"bytes"
)

/*
	Go's SQL Package:
		Tutorial: http://go-database-sql.org/index.html
		Reference: https://golang.org/pkg/database/sql/
*/

var mysql_connect = "cmpe281:Vijay@25021996@tcp(10.0.5.21:3306)/cmpe281"
//var mysql_connect = "root:cmpe281@tcp(localhost:3306)/cmpe281"
var table string = "bitly"


// RabbitMQ Config
//var rabbitmq_server = "localhost"
var rabbitmq_server = "10.0.3.105"
var rabbitmq_port = "5672"
var rabbitmq_queue = "bilty_queue"
var rabbitmq_user = "guest"
var rabbitmq_pass = "guest"

// NewServer configures and returns a Server.
func NewServer() *negroni.Negroni {
	formatter := render.New(render.Options{
		IndentJSON: true,
	})
	n := negroni.Classic()
	mx := mux.NewRouter().StrictSlash(true)
	initRoutes(mx, formatter)
	n.UseHandler(mx)
	return n
}

// Init MySQL DB Connection
func init() {
	// db, err := sql.Open("mysql", mysql_connect)
	// if err != nil {
	// 	log.Fatal(err)
	// } else {
	// 	log.Println("Successfully connected to mysql database")
	// }
	// defer db.Close()

}

// API Routes
func initRoutes(mx *mux.Router, formatter *render.Render) {
	mx.HandleFunc("/ping", pingHandler(formatter)).Methods("GET")
	mx.HandleFunc("/{id}/", redirectHandler(formatter)).Methods("GET")
	mx.HandleFunc("/bitlinks/", getAllUrlHandler(formatter)).Methods("GET")
}

// Helper Functions
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

// API Ping Handler
func pingHandler(formatter *render.Render) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		formatter.JSON(w, http.StatusOK, struct{ Test string }{"REDIRECT SERVER - API version 1.0 alive!"})
	}
}


func redirectHandler(formatter *render.Render) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		params := mux.Vars(req)
		code := params["id"]
		url := urls[code]
		if _, ok := urls[code]; ok {
			fmt.Println("key found")
			//increment count and put in queue
			go handleIncrement(url)
		} else {
			fmt.Println("key not found. Getting from mysql")
			db, err := sql.Open("mysql", mysql_connect)
			defer db.Close()
			if err != nil {
				log.Fatal(err)
			} else {
				rows, _ := db.Query("select * from bitly where code = ?", code)
				defer rows.Close()
				var (
					id        int
					code      string
					shortUrl  string
					longUrl   string
					count	  int
				)
				if rows.Next() {
					rows.Scan(&id, &code, &shortUrl, &longUrl, &count)
					url = MyUrl{	
						Code:   	code,    	
						ShortUrl: 	shortUrl,	    
						LongUrl: 	longUrl,
						Count:      count,
					}
					if urls == nil {
						urls = make(map[string]MyUrl)
					}
					urls[code] = url
				}else{
					log.Println("Entry doesnt exist, creating one")
					if urls == nil {
						urls = make(map[string]MyUrl)
					}
					urls[code] = url
				}
			}
		}
		
		
		http.Redirect(w, req, url.LongUrl, 301)
	}
}


func handleIncrement(url MyUrl) {
	url.Count = url.Count + 1
	if urls == nil {
		urls = make(map[string]MyUrl)
	}
	urls[url.Code] = url
	go queue_send(url)
	//////// update in nosql //////////////

	nlburl := "http://nosql-nlb-1-4a2234afb38d0cd5.elb.us-east-2.amazonaws.com:80/api/"+url.Code
	var jsonStr = []byte(`{"count":`+strconv.Itoa(url.Count)+`}`)
	// payload := strings.NewReader(strconv.Itoa("count:"url.Count))
	req, err := http.NewRequest("PUT", nlburl, bytes.NewBuffer(jsonStr))
	if err != nil {
		log.Printf("error connecting to nosql NLB")
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("failed to hit http request for nosql server")
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func getAllUrlHandler(formatter *render.Render) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		var url_array [] MyUrl
		db, err := sql.Open("mysql", mysql_connect)
		defer db.Close()
		if err != nil {
			log.Fatal(err)
		} else {
			rows, _ := db.Query("select * from bitly")
			defer rows.Close()
			var url MyUrl
			var (
				id        int
				code      string
				shortUrl  string
				longUrl   string
			)

			for rows.Next() {
				rows.Scan(&id, &code, &shortUrl, &longUrl)
				url = MyUrl{
					Id: 		id,	
					Code:   	code,    	
					ShortUrl: 	shortUrl,	    
					LongUrl: 	longUrl,
				}
				url_array = append(url_array, url)
			}
		}

		formatter.JSON(w, http.StatusOK, url_array)
	}
}

func queue_receive() {
	conn, err := amqp.Dial("amqp://"+rabbitmq_user+":"+rabbitmq_pass+"@"+rabbitmq_server+":"+rabbitmq_port+"/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"bitly_exchange",   // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"nosql_queue", // name
		true,   // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name, // queue name
		"",     // routing key
		"bitly_exchange", // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",    // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			var url MyUrl
			json.Unmarshal(d.Body, &url)

			code := url.ShortUrl[len(url.ShortUrl) - 8: len(url.ShortUrl) - 1]
			if urls == nil {
				urls = make(map[string]MyUrl)
			}
			urls[code] = url

			/////// push to nosql //////

			nlburl := "http://nosql-nlb-1-4a2234afb38d0cd5.elb.us-east-2.amazonaws.com:80/api/"+url.Code
			var jsonStr = []byte(`{"count":`+strconv.Itoa(url.Count)+`}`)
			// payload := strings.NewReader(strconv.Itoa("count:"url.Count))
			req, err := http.NewRequest("POST", nlburl, bytes.NewBuffer(jsonStr))
			if err != nil {
				log.Printf("error connecting to nosql NLB")
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("error connecting to pushing data")
			}
			defer resp.Body.Close()
			body, _ := ioutil.ReadAll(resp.Body)
			fmt.Println(string(body))
		}
	}()
	<-forever
}


func queue_send(message MyUrl) {
	conn, err := amqp.Dial("amqp://"+rabbitmq_user+":"+rabbitmq_pass+"@"+rabbitmq_server+":"+rabbitmq_port+"/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"mysql_exchange",   // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	reqBodyBytes := new(bytes.Buffer)
	json.NewEncoder(reqBodyBytes).Encode(message)
	body := reqBodyBytes.Bytes()

	err = ch.Publish(
		"mysql_exchange",     // exchange
		"", 				// routing key
		false,  			// mandatory
		false,  			// immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})

}


