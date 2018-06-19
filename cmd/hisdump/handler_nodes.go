package main

import (
	"fmt"
	"log"
	"math"

	"github.com/paulmach/osm"
)

func nodesHandler(nodes chan *osm.Node, done chan bool) {
	_, err := db.Prepare("createNodes", `INSERT INTO nodes (node_id, latitude, longitude, changeset_id, visible, timestamp, tile, version)
	VALUES($1,$2,$3,$4,$5,$6,$7,$8)
	ON CONFLICT (node_id, version) DO UPDATE
	SET latitude=excluded.latitude,longitude=excluded.longitude,changeset_id=excluded.changeset_id,
	visible=excluded.visible,timestamp=excluded.timestamp,tile=excluded.tile,version=excluded.version;`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Prepare("createNodeTags", `INSERT INTO node_tags (node_id, version, k, v)
	VALUES($1,$2,$3,$4)
	ON CONFLICT (node_id, version, k) DO UPDATE
	SET v = excluded.v;`)
	if err != nil {
		log.Fatal(err)
	}

	for n := range nodes {
		if _, err := nodesTx.Exec("createUser", fmt.Sprintf("user-%v@hisdump.com", n.UserID), n.UserID, n.User, n.User); err != nil {
			nodesTx.Rollback() // return an error too, we may want to wrap them
			log.Fatal("Node createUser", err)
		}

		if _, err := nodesTx.Exec("createChangeset", n.ChangesetID, n.UserID); err != nil {
			nodesTx.Rollback() // return an error too, we may want to wrap them
			log.Fatal("Node createChangeset", err)
		}

		lat := int64(n.Lat * math.Pow10(7))
		lon := int64(n.Lon * math.Pow10(7))
		if _, err := nodesTx.Exec("createNodes", n.ID, lat, lon, n.ChangesetID, n.Visible, n.Timestamp, 100000, n.Version); err != nil {
			nodesTx.Rollback() // return an error too, we may want to wrap them
			log.Fatal("createNodes", err)
		}

		for k := range n.Tags.Map() {
			if _, err := nodesTx.Exec("createNodeTags", n.ID, n.Version, k, n.Tags.Find(k)); err != nil {
				nodesTx.Rollback() // return an error too, we may want to wrap them
				log.Fatal("createNodeTags", err)
			}
		}
	}

	done <- true
}
