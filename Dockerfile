FROM heroiclabs/nakama:3.22.0

COPY ./nakama/data /nakama/data
COPY ./modules /nakama/data/modules

ENTRYPOINT ["/bin/sh", "-ecx", "/nakama/nakama migrate up --database.address $DB_URL && exec /nakama/nakama --config /nakama/data/config.yml --database.address $DB_URL"]

EXPOSE 7349 7350 7351