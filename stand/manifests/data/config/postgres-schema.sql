CREATE TABLE events (
  id           TEXT PRIMARY KEY,
  tenant       INT         NOT NULL,
  type         TEXT        NOT NULL,
  segment      TEXT        NOT NULL,
  status       TEXT        NOT NULL,
  received_at  TIMESTAMPTZ NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX events_tenant_processed_idx ON events (tenant, processed_at DESC);

CREATE TABLE tenant_stats (
  tenant        INT PRIMARY KEY,
  processed     BIGINT      NOT NULL,
  failed        BIGINT      NOT NULL,
  last_event_at TIMESTAMPTZ NOT NULL
);