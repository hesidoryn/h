package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/paulmach/osm"
)

func relationsHandler(relations chan *osm.Relation, done chan bool) {
	_, err := relationsTx.Prepare("createRelations", `INSERT INTO relations (relation_id, changeset_id, visible, timestamp, version)
	VALUES($1,$2,$3,$4,$5)
	ON CONFLICT (relation_id, version) DO UPDATE
	SET changeset_id=excluded.changeset_id,visible=excluded.visible,
	timestamp=excluded.timestamp,version=excluded.version;`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = relationsTx.Prepare("createRelationTags", `INSERT INTO relation_tags (relation_id, version, k, v)
	VALUES($1,$2,$3,$4)
	ON CONFLICT (relation_id, version, k) DO UPDATE
	SET v = excluded.v;`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = relationsTx.Prepare("createRelationMembers", `INSERT INTO relation_members (relation_id, member_type, member_id, member_role, version, sequence_id)
	VALUES($1,$2,$3,$4,$5,$6)
	ON CONFLICT DO NOTHING;`)
	if err != nil {
		log.Fatal(err)
	}

	for r := range relations {
		if _, err := relationsTx.Exec("createUser", fmt.Sprintf("user-%v@hisdump.com", r.UserID), r.UserID, r.User, r.User); err != nil {
			relationsTx.Rollback() // return an error too, we may want to wrap them
			log.Fatal(err)
		}

		if _, err := relationsTx.Exec("createChangeset", r.ChangesetID, r.UserID); err != nil {
			relationsTx.Rollback() // return an error too, we may want to wrap them
			log.Fatal(err)
		}

		if _, err := relationsTx.Exec("createRelations", r.ID, r.ChangesetID, r.Visible, r.Timestamp, r.Version); err != nil {
			relationsTx.Rollback() // return an error too, we may want to wrap them
			log.Fatal(err)
		}

		for k := range r.Tags.Map() {
			if _, err := relationsTx.Exec("createRelationTags", r.ID, r.Version, k, r.Tags.Find(k)); err != nil {
				relationsTx.Rollback() // return an error too, we may want to wrap them
				log.Fatal(err)
			}
		}

		for id, m := range r.Members {
			if _, err := relationsTx.Exec("createRelationMembers", r.ID, strings.Title(string(m.Type)), m.Ref, m.Role, r.Version, id+1); err != nil {
				relationsTx.Rollback() // return an error too, we may want to wrap them
				log.Fatal(err)
			}
		}
	}

	done <- true
}
