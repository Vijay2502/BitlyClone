package main

import (
	"fmt"
	"log"
	"time"
	"net/http"
	"database/sql"
	"encoding/json"
	"bytes"
	"github.com/codegangsta/negroni"
	"github.com/streadway/amqp"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"github.com/speps/go-hashids"
)

/*
	Go's SQL Package:
		Tutorial: http://go-database-sql.org/index.html
		Reference: https://golang.org/pkg/database/sql/
*/

//var mysql_connect = "root:cmpe281@tcp(localhost:3306)/cmpe281"
var mysql_connect = "cmpe281:Vijay@25021996@tcp(10.0.5.21:3306)/cmpe281"
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
	db, err := sql.Open("mysql", mysql_connect)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("Successfully connected to mysql database")
	}
	defer db.Close()

}

// API Routes
func initRoutes(mx *mux.Router, formatter *render.Render) {
	mx.HandleFunc("/ping", pingHandler(formatter)).Methods("GET")
	mx.HandleFunc("/shorten", urlShortenerHandler(formatter)).Methods("POST")
	mx.HandleFunc("/expand/", expandUrlHandler(formatter)).Methods("GET")
}

// Helper Functions
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func isUsed(code string) bool {
	db, err := sql.Open("mysql", mysql_connect)
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	} else {
		_, err := db.Query("select id from bitly where code = ?", code)
		if err != nil {
			return false
		 }
		 return true
	}
	return false
}

// API Ping Handler
func pingHandler(formatter *render.Render) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		formatter.JSON(w, http.StatusOK, struct{ Test string }{"API version 1.0 alive!"})
	}
}

// API URL shortener Handeler
func urlShortenerHandler(formatter *render.Render) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		var url MyUrl
		_ = json.NewDecoder(req.Body).Decode(&url)

		db, err := sql.Open("mysql", mysql_connect)
		defer db.Close()
		if err != nil {
			log.Fatal(err)
		} else {
			rows, _ := db.Query("select id, code, short_url, long_url from bitly where long_url = ?", url.LongUrl)
			defer rows.Close()

			var (
				id        int
				code      string
				shortUrl  string
				longUrl   string
			)

			if rows.Next() {
				rows.Scan(&id, &code, &shortUrl, &longUrl)
				url = MyUrl{	
					Code:   	code,    	
					ShortUrl: 	shortUrl,	    
					LongUrl: 	longUrl,
				}
			}
			if len(longUrl) == 0 {
				hd := hashids.NewData()
				h,_ := hashids.NewWithData(hd)
				now := time.Now()
				url.Code, _ = h.Encode([]int{int(now.Unix())})
				//url.ShortUrl := "protocol" + "://" + "serverIP:port/" + code

				/////
				/// code to check the code exists in db  ////
				////

				//// kong ip 18.222.202.23//
				url.ShortUrl = "http://18.222.202.23:8000/" + "redirect/" + url.Code + "/"
				//url.ShortUrl = "http://3.133.120.81:3001/" + url.Code + "/"
				//url.ShortUrl = "http://localhost:3001/" + url.Code + "/"
				stmt, error := db.Prepare("insert into bitly (code, short_url, long_url) values(?,?,?)")
				if error != nil {
					panic(error.Error())
				}

				go queue_send(url)

				_, er := stmt.Exec(url.Code, url.ShortUrl, url.LongUrl)
				url.Count = 0;
				if er != nil {
					panic(er.Error())
				}
			} 		
		}
		
		formatter.JSON(w, http.StatusOK, url)
	}
}

func expandUrlHandler(formatter *render.Render) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		params := req.URL.Query()
		var shortUrl string = params.Get("shortUrl")
		var url MyUrl

		db, err := sql.Open("mysql", mysql_connect)
		defer db.Close()
		if err != nil {
			log.Fatal(err)
		} else {
			rows, _ := db.Query("select * from bitly where short_url = ?", shortUrl)
			defer rows.Close()

			var (
				id        int
				code      string
				shortUrl  string
				longUrl   string
			)

			if rows.Next() {
				rows.Scan(&id, &code, &shortUrl, &longUrl)
				url = MyUrl{	
					Code:   	code,    	
					ShortUrl: 	shortUrl,	    
					LongUrl: 	longUrl,
				}

				log.Println(url)
			}
		}
		
		formatter.JSON(w, http.StatusOK, url)
	}
}

func queue_send(message MyUrl) {
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

	reqBodyBytes := new(bytes.Buffer)
	json.NewEncoder(reqBodyBytes).Encode(message)
	body := reqBodyBytes.Bytes()

	err = ch.Publish(
		"bitly_exchange",     // exchange
		"", 				// routing key
		false,  			// mandatory
		false,  			// immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})

}

func queue_receive() {
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

	q, err := ch.QueueDeclare(
		"mysql_queue", // name
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
		"mysql_exchange", // exchange
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
	log.Printf("here")
	forever := make(chan bool)

	go func() {
		db, err := sql.Open("mysql", mysql_connect)
		defer db.Close()
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			var url MyUrl
			json.Unmarshal(d.Body, &url)

			//code := url.ShortUrl[len(url.ShortUrl) - 8: len(url.ShortUrl) - 1]
			log.Println(url)
			// update count in mysql
			if err != nil {
				log.Fatal(err)
			} else {
				sqlStatement, err := db.Prepare("UPDATE bitly SET count = count + 1 WHERE short_url = ?")
				_, err = sqlStatement.Exec(url.ShortUrl)
				if err != nil {
					panic(err)
				}
			}
		}
	}()
	<-forever
}

/*
	mysql --user=cmpe281 --password
	Vijay@25021996

	create database cmpe281 ;

	use cmpe281;

	CREATE TABLE bitly (
	  id bigint(20) NOT NULL AUTO_INCREMENT,
	  code varchar(255) NOT NULL,
	  short_url varchar(255) NOT NULL,
	  long_url varchar(255) NOT NULL,
	  count bigint(20),
	  PRIMARY KEY (id),
	  UNIQUE KEY code (code)
	) ;

	docker run --name rabbitmq --restart always -p 8080:15672 -p 4369:4369 -p 5672:5672 -d rabbitmq:3-management

	docker run -d --name mysql -td -p 3306:3306 -e MYSQL_ROOT_PASSWORD=cmpe281 mysql:5.5 
	mysql --user=root --password=cmpe281

	docker run --name nosql -td -p 9090:9090 -p 8888:8888 vijay2502/nosql

	docker run --name nosql --restart always -e IP_ADDRESS=`curl http://169.254.169.254/latest/meta-data/local-ipv4` -td -p 9090:9090 -p 8888:8888 vijay2502/nosql

	docker run --name redirect --restart always -td -p 3001:3001 vijay2502/redirect
	docker run --name bitly --restart always -td -p 3000:3000 vijay2502/bitly
*/
