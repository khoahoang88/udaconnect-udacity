export CT_DB_USERNAME=ct_admin
export CT_DB_NAME=geoconnections


cat ./db/init-location-db.sql | kubectl exec -i $1 -- bash -c "psql -U $CT_DB_USERNAME -d $CT_DB_NAME"
