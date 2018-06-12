package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"

	_ "github.com/lib/pq"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
)

func main() {
	db, err := sql.Open("postgres", "user=heorhi dbname=gis sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	f, err := os.Open("./osm.pbf")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	scanner := osmpbf.New(context.Background(), f, 3)
	defer scanner.Close()

	for scanner.Scan() {
		o := scanner.Object()
		if o.ObjectID().Type() == osm.TypeNode {
			n := o.(*osm.Node)
			fmt.Println(n)
			tx, err := db.Begin()
			if err != nil {
				log.Fatal(err)
			}

			{
				stmt, err := tx.Prepare(`INSERT INTO users (email, id, pass_crypt, creation_time, display_name, data_public, description, home_lat, home_lon, pass_salt)
					 VALUES($1,$2,'00000000000000000000000000000000',CURRENT_TIMESTAMP,$3,true,$4,0,0,'00000000')
					 ON CONFLICT (id) DO NOTHING;`)
				if err != nil {
					log.Fatal(err)
				}
				defer stmt.Close()

				if _, err := stmt.Exec(fmt.Sprintf("user-%v@hisdump.com", n.UserID), n.UserID, n.User, n.User); err != nil {
					tx.Rollback() // return an error too, we may want to wrap them
					log.Fatal(err)
				}
			}

			{
				stmt, err := tx.Prepare(`INSERT INTO changesets (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at)
					 VALUES ($1, $2, CURRENT_TIMESTAMP, -900000000, 900000000, -1800000000, 1800000000, CURRENT_TIMESTAMP)
					 ON CONFLICT (id) DO NOTHING;`)
				if err != nil {
					log.Fatal(err)
				}
				defer stmt.Close()

				if _, err := stmt.Exec(n.ChangesetID, n.UserID); err != nil {
					tx.Rollback() // return an error too, we may want to wrap them
					log.Fatal(err)
				}
			}

			{
				stmt, err := tx.Prepare(`INSERT INTO current_nodes (id, latitude, longitude, changeset_id, visible, timestamp, tile, version)
					 VALUES($1,$2,$3,$4,$5,$6,$7,$8)
					 ON CONFLICT (id) DO UPDATE
					 SET latitude=excluded.latitude,longitude=excluded.longitude,changeset_id=excluded.changeset_id,
					 visible=excluded.visible,timestamp=excluded.timestamp,tile=excluded.tile,version=excluded.version
					 WHERE excluded.version > version;`)
				if err != nil {
					log.Fatal(err)
				}
				defer stmt.Close()

				lat := float64(int64(n.Lat * math.Pow10(7)))
				lon := float64(int64(n.Lon * math.Pow10(7)))
				if _, err := stmt.Exec(n.ID, lat, lon, n.ChangesetID, n.Visible, n.Timestamp, 100000, n.Version); err != nil {
					tx.Rollback() // return an error too, we may want to wrap them
					log.Fatal(err)
				}
			}

			{
				stmt, err := tx.Prepare(`INSERT INTO current_node_tags (node_id, k, v)
					 VALUES($1,$2,$3)
					 ON CONFLICT (node_id, k) DO UPDATE
					 SET v = excluded.version;`)
				if err != nil {
					log.Fatal(err)
				}
				defer stmt.Close()

				for k := range n.Tags.Map() {
					if _, err := stmt.Exec(n.ID, k, n.Tags.Find(k)); err != nil {
						tx.Rollback() // return an error too, we may want to wrap them
						log.Fatal(err)
					}
				}
			}

			err = tx.Commit()
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	if scanner.Err() != nil {
		panic(err)
	}
}
