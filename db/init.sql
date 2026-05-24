CREATE TABLE option_snapshots (
    snapshot_ts        TIMESTAMP NOT NULL,
    instrument_name    TEXT NOT NULL,

    mark_price         DOUBLE PRECISION,
    forward_price      DOUBLE PRECISION,

    askVol             REAL,
    bidVol             REAL,
    markVol            REAL,
    volLv              REAL,
    realVol            REAL,

    delta              DOUBLE PRECISION,
    deltaBS            DOUBLE PRECISION,
    gamma              DOUBLE PRECISION,
    gammaBS            DOUBLE PRECISION,
    vega               DOUBLE PRECISION,
    vegaBS             DOUBLE PRECISION,
    theta              DOUBLE PRECISION,
    thetaBS            DOUBLE PRECISION,

    distance           REAL,
    leverage           REAL,

    buyApr             REAL,
    sellApr            REAL,

    CONSTRAINT option_snapshots_pk
    PRIMARY KEY (snapshot_ts, instrument_name)
);

CREATE INDEX idx_snapshot_time
ON option_snapshots(snapshot_ts);

CREATE INDEX idx_instr_name
ON option_snapshots (instrument_name);


CREATE TABLE open_interest (
    snapshot_ts        TIMESTAMP NOT NULL,
    instrument_name    TEXT NOT NULL,
    
    oi                 DOUBLE PRECISION,
    oiCcy              DOUBLE PRECISION,
    oiUsd              DOUBLE PRECISION,
    price              DOUBLE PRECISION,	

    CONSTRAINT open_interest_pk 
    PRIMARY KEY (snapshot_ts, instrument_name)
);
