/*
Package parser
*/

package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/andybalholm/cascadia"
	"github.com/tealeg/xlsx"
	"golang.org/x/net/html"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/redis.v5"
)

var (
	client   *redis.Client
	channels *mgo.Collection
)

const (
	searchKey = "search"
	pagesKey  = "pages"
)

type youtuber struct {
	Name        string
	URL         string
	Subscribers uint64
	Description string
	Image       string
	Created     time.Time
	Term        string
	Category    string
}

type pageRequest struct {
	URL      string
	Category string
	Term     string
}

type searchRequest struct {
	Category string
	Term     string
}

func query(path string) (*html.Node, error) {
	resp, err := http.Get(path)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	doc, err := html.Parse(resp.Body)

	if err != nil {
		return nil, err
	}

	return doc, nil
}

func searchList(url string) []string {
	result := make([]string, 0, 30)

	doc, err := query(url)
	if err != nil {
		log.Println("Error loading channals by url", url, err)
		return result
	}

	divs := cascadia.MustCompile(".qualified-channel-title-wrapper .yt-uix-sessionlink").MatchAll(doc)

	for _, div := range divs {
		for _, attr := range div.Attr {
			if attr.Key == "href" {
				result = append(result, attr.Val)
			}
		}
	}

	return result
}

func getText(doc *html.Node, selector string) string {
	node := cascadia.MustCompile(selector).MatchFirst(doc)

	if node == nil || node.FirstChild == nil {
		return ""
	}

	return node.FirstChild.Data
}

func getAttr(doc *html.Node, selector, attr string) string {
	node := cascadia.MustCompile(selector).MatchFirst(doc)

	if node == nil || len(node.Attr) == 0 {
		return ""
	}

	for _, attr := range node.Attr {
		if attr.Key == "href" {
			return attr.Val
		}
	}

	return ""
}

func exists(request *pageRequest) bool {
	result := youtuber{}
	query := bson.M{
		"url":      request.URL,
		"category": request.Category,
	}
	err := channels.Find(query).One(&result)
	if err == mgo.ErrNotFound {
		return false
	}
	if err != nil {
		addPage(request.URL, request.Category, request.Term)
		log.Fatal("Error checking existance", err)
	}

	return true
}

func profilePage(request *pageRequest) {
	if exists(request) {
		// log.Println("Pre-check duplicate", request.URL)
		return
	}
	url := request.URL

	aboutURL := url + "/about"
	doc, err := query(aboutURL)
	if err != nil {
		log.Println("Error loading profile", aboutURL, err)
		return
	}

	r := strings.NewReplacer("\u00a0", "", " ", "", ",", "")
	subscribers := r.Replace(getText(doc, ".subscribed"))
	if subscribers == "" {
		subscribers = "0"
	}
	i, err := strconv.ParseUint(subscribers, 10, 64)
	if err != nil {
		fmt.Println("Error parsing subscribers", err)
	}
	if i < 1000 {
		return
	}
	description := getText(doc, ".about-description pre")

	name := getText(doc, ".branded-page-header-title-link")
	image := getAttr(doc, "link[itemprop='thumbnailUrl']", "href")

	err = channels.Insert(&youtuber{name, url, i, description, image, time.Now(), request.Term, request.Category})
	if mgo.IsDup(err) {
		// log.Println("Duplicate", name)
		return
	}
	if err != nil {
		addPage(url, request.Category, request.Term)
		log.Fatal("Database write failed", err)
	}
	log.Println("Parsed channel", name)

	return
}

func parseSearchPages(request *searchRequest) {
	for i := 1; i <= 34; i++ {
		URL, err := url.Parse("https://www.youtube.com/channels")
		if err != nil {
			log.Println("Unexpected error", err)
			continue
		}
		query := url.Values{}
		query.Add("q", request.Term)
		query.Add("page", strconv.Itoa(i))
		URL.RawQuery = query.Encode()
		result := searchList(URL.String())
		log.Println("Parsed search term", request.Term, "page", strconv.Itoa(i))
		if len(result) == 0 && i < 33 {
			i = 33
			continue
		}
		for _, item := range result {
			url := "https://www.youtube.com" + item
			addPage(url, request.Category, request.Term)
		}
	}
}

func addPage(url, category, term string) {
	message := url + ";" + category + ";" + term
	err := client.RPush(pagesKey, message).Err()
	if err != nil {
		log.Fatal("Push page to list error", err)
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	mongodbURI := os.Getenv("MONGODB_URI")
	if mongodbURI == "" {
		mongodbURI = "localhost"
	}
	redisURI := os.Getenv("REDISTOGO_URL")
	var opts *redis.Options
	fmt.Println("URI ", redisURI)
	if redisURI == "" {
		opts = &redis.Options{
			Addr:     "127.0.0.1:6379",
			Password: "",
			DB:       0,
		}
	} else {
		opts = &redis.Options{
			Addr:     "jack.redistogo.com:9074",
			Password: "be8c21c6e8515adfe7153482c12135e1",
			DB:       0,
		}
	}
	client = redis.NewClient(opts)

	log.Println("Connecting to mongodb")

	session, err := mgo.Dial(mongodbURI)
	if err != nil {
		log.Fatal("Connection to database failed", err)
	}

	defer session.Close()
	log.Println("Connected to mongodb")

	channels = session.DB("").C("channels")

	index := mgo.Index{
		Key:        []string{"url", "category"},
		Unique:     true,
		Background: true,
		Sparse:     true,
	}

	if err := channels.EnsureIndex(index); err != nil {
		log.Fatal("Ensure index error", err)
	}

	go func() {
		for {
			result, err := client.BLPop(10*time.Second, pagesKey).Result()
			if err == redis.Nil {
				continue
			}
			if err != nil {
				log.Println("Error reading prom pages list", err)
				continue
			}
			splited := strings.Split(result[1], ";")

			go profilePage(&pageRequest{splited[0], splited[1], splited[2]})
		}
	}()

	go func() {
		for {
			result, err := client.BLPop(10*time.Second, searchKey).Result()
			if err == redis.Nil {
				continue
			}
			if err != nil {
				log.Println("Read from queue error", err)
				log.Fatal("Error reading from search list", err)
				continue
			}
			splited := strings.Split(result[1], ";")

			parseSearchPages(&searchRequest{splited[1], splited[0]})
		}
	}()

	http.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		terms := strings.Split(r.FormValue("terms"), ",")
		category := r.FormValue("category")
		if category == "" {
			category = "none"
		}

		if terms[0] == "" {
			fmt.Fprintf(w, "mismatch term params")
			return
		}

		for _, term := range terms {
			term = strings.Replace(term, ";", " ", -1)
			err := client.RPush(searchKey, term+";"+category).Err()
			if err != nil {
				fmt.Fprintf(w, "Push to queue error")
				return
			}
		}

		fmt.Fprintf(w, "got it")
	})

	http.HandleFunc("/youtubers", func(w http.ResponseWriter, r *http.Request) {
		query := bson.M{}
		category := r.URL.Query().Get("category")
		if category != "" {
			query = bson.M{"category": category}
		}

		var result []youtuber
		err := channels.Find(query).All(&result)
		if err != nil {
			fmt.Fprintf(w, "Some error occured")
			return
		}
		file := xlsx.NewFile()
		sheet, err := file.AddSheet("Sheet1")
		if err != nil {
			log.Println("Error adding sheet", err)
			fmt.Fprintf(w, "Some error occured")
			return
		}

		// @TODO refactor
		headerRow := sheet.AddRow()

		headerRow.AddCell().Value = "Name"
		headerRow.AddCell().Value = "Url"
		headerRow.AddCell().Value = "Subscribers"
		headerRow.AddCell().Value = "Description"
		headerRow.AddCell().Value = "Image"
		headerRow.AddCell().Value = "Date parsed"
		headerRow.AddCell().Value = "Term"
		headerRow.AddCell().Value = "Category"

		for _, item := range result {
			row := sheet.AddRow()
			row.AddCell().Value = item.Name
			row.AddCell().Value = item.URL
			row.AddCell().Value = strconv.FormatUint(item.Subscribers, 10)
			row.AddCell().Value = item.Description
			row.AddCell().Value = item.Image
			row.AddCell().Value = item.Created.String()
			row.AddCell().Value = item.Term
			row.AddCell().Value = item.Category
		}

		w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
		w.Header().Set("Content-Disposition", "attachment;filename=channels.xlsx")
		err = file.Write(w)
		if err != nil {
			log.Println("Error sending xlsx", err)
		}
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		search, err := client.LLen(searchKey).Result()
		if err != nil {
			fmt.Fprintf(w, "Error getting search length")
			return
		}
		if search == 0 {
			fmt.Fprintf(w, "Done")
			return
		}
		fmt.Fprintf(w, "Pending "+strconv.FormatInt(search, 10))
	})

	log.Fatal(http.ListenAndServe(":"+port, nil))
}
