package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx"
	_ "github.com/lib/pq"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
)

var (
	db          *pgx.ConnPool
	nodesTx     *pgx.Tx
	waysTx      *pgx.Tx
	relationsTx *pgx.Tx
)

func init() {
	config := pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     "localhost",
			Database: "gis",
			User:     "heorhi",
		},
		MaxConnections: 3,
	}

	var err error
	db, err = pgx.NewConnPool(config)
	if err != nil {
		panic(err)
	}

	_, err = db.Prepare("createUser", `INSERT INTO users (email, id, pass_crypt, creation_time, display_name, data_public, description, home_lat, home_lon, pass_salt)
	VALUES($1,$2,'00000000000000000000000000000000',CURRENT_TIMESTAMP,$3,true,$4,0,0,'00000000')
	ON CONFLICT (id) DO NOTHING;`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Prepare("createChangeset", `INSERT INTO changesets (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at)
	VALUES ($1, $2, CURRENT_TIMESTAMP, -900000000, 900000000, -1800000000, 1800000000, CURRENT_TIMESTAMP)
	ON CONFLICT (id) DO NOTHING;`)
	if err != nil {
		log.Fatal(err)
	}

	nodesTx, err = db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	waysTx, err = db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	relationsTx, err = db.Begin()
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	defer db.Close()
	defer nodesTx.Commit()
	defer waysTx.Commit()
	defer relationsTx.Commit()

	f, err := os.Open("./osm.pbf")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	scanner := osmpbf.New(context.Background(), f, 3)
	defer scanner.Close()

	nodes := make(chan *osm.Node)
	ways := make(chan *osm.Way)
	relations := make(chan *osm.Relation)
	nodesDone := make(chan bool)
	waysDone := make(chan bool)
	relationsDone := make(chan bool)

	go nodesHandler(nodes, nodesDone)
	go waysHandler(ways, waysDone)
	go relationsHandler(relations, relationsDone)

	for scanner.Scan() {
		o := scanner.Object()
		fmt.Println(o)
		if o.ObjectID().Type() == osm.TypeNode {
			fmt.Println("here1")
			nodes <- o.(*osm.Node)
			fmt.Println("here2")
			continue
		}
		if o.ObjectID().Type() == osm.TypeWay {
			ways <- o.(*osm.Way)
			continue
		}
		if o.ObjectID().Type() == osm.TypeRelation {
			fmt.Println(o)
			relations <- o.(*osm.Relation)
			continue
		}
	}
	if scanner.Err() != nil {
		panic(err)
	}

	close(nodes)
	close(ways)
	close(relations)

	<-relationsDone
	fmt.Println("Relations import is done")
	<-waysDone
	fmt.Println("Ways import is done")
	<-nodesDone
	fmt.Println("Nodes import is done")
}
