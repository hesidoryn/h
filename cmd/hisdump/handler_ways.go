package main

import (
	"fmt"
	"log"

	"github.com/paulmach/osm"
)

func waysHandler(ways chan *osm.Way, done chan bool) {
	_, err := waysTx.Prepare("createWays", `INSERT INTO ways (way_id, changeset_id, visible, timestamp, version)
	VALUES($1,$2,$3,$4,$5)
	ON CONFLICT (way_id, version) DO UPDATE
	SET changeset_id=excluded.changeset_id,visible=excluded.visible,
	timestamp=excluded.timestamp,version=excluded.version;`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = waysTx.Prepare("createWayTags", `INSERT INTO way_tags (way_id, version, k, v)
	VALUES($1,$2,$3,$4)
	ON CONFLICT (way_id, version, k) DO UPDATE
	SET v = excluded.v;`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = waysTx.Prepare("createWayNodes", `INSERT INTO way_nodes (way_id, node_id, version, sequence_id)
	VALUES($1,$2,$3,$4)
	ON CONFLICT (way_id, version, sequence_id) DO UPDATE
	SET node_id = excluded.node_id;`)
	if err != nil {
		log.Fatal(err)
	}

	for w := range ways {
		if _, err := waysTx.Exec("createUser", fmt.Sprintf("user-%v@hisdump.com", w.UserID), w.UserID, w.User, w.User); err != nil {
			waysTx.Rollback() // return an error too, we may want to wrap them
			log.Fatal(err)
		}

		if _, err := waysTx.Exec("createChangeset", w.ChangesetID, w.UserID); err != nil {
			waysTx.Rollback() // return an error too, we may want to wrap them
			log.Fatal(err)
		}

		if _, err := waysTx.Exec("createWays", w.ID, w.ChangesetID, w.Visible, w.Timestamp, w.Version); err != nil {
			waysTx.Rollback() // return an error too, we may want to wrap them
			log.Fatal(err)
		}

		for k := range w.Tags.Map() {
			if _, err := waysTx.Exec("createWayTags", w.ID, w.Version, k, w.Tags.Find(k)); err != nil {
				waysTx.Rollback() // return an error too, we may want to wrap them
				log.Fatal(err)
			}
		}

		for id, n := range w.Nodes {
			if _, err := waysTx.Exec("createWayNodes", w.ID, n.ID, w.Version, id+1); err != nil {
				waysTx.Rollback() // return an error too, we may want to wrap them
				log.Fatal(err)
			}
		}
	}
	done <- true
}
