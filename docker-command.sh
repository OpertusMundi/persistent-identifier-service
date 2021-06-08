#!/bin/bash
set -u -e -o pipefail

[[ "${DEBUG:-f}" != "f" || "${XTRACE:-f}" != "f" ]] && set -x

#
# Generate configuration
#

db_url_pattern="^postgresql://([a-z][-a-z0-9]*([.][a-z][-a-z0-9]*)*):([1-9][0-9]*)/(.+)"
db_host=$(echo ${DATABASE_URL} | sed -n -r "s,${db_url_pattern},\1,p")
db_port=$(echo ${DATABASE_URL} | sed -n -r "s,${db_url_pattern},\3,p")
db_name=$(echo ${DATABASE_URL} | sed -n -r "s,${db_url_pattern},\4,p")
db_user=${DATABASE_USERNAME}
db_password=$(cat ${DATABASE_PASSWORD_FILE} | tr -d '\n')

settings_sed_script=$(mktemp --suffix=.sed)
cat >${settings_sed_script} <<EOD
/^postgresql:/,/^[a-z].*:/ {
   s,((\s+)host): (.*),\1: ${db_host},
   s,((\s+)port): (.*),\1: ${db_port},
   s,((\s+)db): (.*),\1: ${db_name},
   s,((\s+)user): (.*),\1: ${db_user},
   s,((\s+)password): (.*),\1: ${db_password},
}
EOD

sed -i -r -f ${settings_sed_script} settings.yml 

#
# Run
#

app="ompid:app"
exec uvicorn --host "0.0.0.0" --no-use-colors --access-log ${app}
