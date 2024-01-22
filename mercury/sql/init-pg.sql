CREATE SEQUENCE IF NOT EXISTS mercury_spaces_id_seq;

CREATE TABLE IF NOT EXISTS mercury_spaces
(
    space character varying NOT NULL,
    id integer NOT NULL DEFAULT nextval('mercury_spaces_id_seq'::regclass),
    notes character varying[] NOT NULL DEFAULT '{}'::character varying[],
    tags character varying[] NOT NULL DEFAULT '{}'::character varying[],
    CONSTRAINT mercury_namespace_pk PRIMARY KEY (id)
);
CREATE UNIQUE INDEX IF NOT EXISTS mercury_namespace_space_uindex
    ON mercury_spaces USING btree
    (space ASC NULLS LAST);

CREATE TABLE IF NOT EXISTS mercury_values
(
    id integer NOT NULL,
    seq integer NOT NULL,
    name character varying NOT NULL,
    "values" character varying[] NOT NULL DEFAULT '{}'::character varying[],
    tags character varying[] NOT NULL DEFAULT '{}'::character varying[],
    notes character varying[] NOT NULL DEFAULT '{}'::character varying[],
    CONSTRAINT mercury_values_pk PRIMARY KEY (id, seq)
);
CREATE INDEX IF NOT EXISTS mercury_values_name_index
    ON mercury_values USING btree
    (name ASC NULLS LAST);

CREATE OR REPLACE VIEW mercury_registry_vw
 AS
 SELECT 
    s.id,
    v.seq,
    s.space,
    v.name,
    v."values",
    v.notes,
    v.tags
 FROM mercury_spaces s
 JOIN mercury_values v ON s.id = v.id;

CREATE OR REPLACE VIEW mercury_groups_vw
 AS
 SELECT DISTINCT 
    unnest(vw."values") AS user_id,
    vw.name AS group_id
 FROM mercury_registry_vw vw
 WHERE vw.space::text = 'mercury.groups'::text;

CREATE OR REPLACE VIEW mercury_group_rules_vw
 AS
 WITH 
 tt as (
    SELECT DISTINCT
        vw.name AS group_id,
        unnest(vw."values") AS rules
    FROM mercury_registry_vw vw
    WHERE vw.space::text = 'mercury.policy'::text
 )
 SELECT tt.group_id,
    split_part(tt.rules::text, ' '::text, 1) AS role,
    split_part(tt.rules::text, ' '::text, 2) AS type,
    split_part(tt.rules::text, ' '::text, 3) AS match
 FROM tt;

CREATE OR REPLACE VIEW mercury_user_rules_vw
 AS
 WITH
 tt as (
    SELECT DISTINCT 
        vw.name AS group_id,
        unnest(vw."values") AS rules
    FROM mercury_registry_vw vw
    WHERE vw.space::text = 'mercury.policy'::text
 )
 SELECT 
    g.user_id,
    split_part(tt.rules::text, ' '::text, 1) AS role,
    split_part(tt.rules::text, ' '::text, 2) AS type,
    split_part(tt.rules::text, ' '::text, 3) AS match
 FROM mercury_groups_vw g
 JOIN tt ON g.group_id::text = tt.group_id::text;

CREATE OR REPLACE VIEW mercury_rules_vw
 AS
 SELECT 
    'U-'::text || vw.user_id::text AS id,
    vw.role,
    vw.type,
    vw.match
 FROM mercury_user_rules_vw vw
 UNION
 SELECT 
    'G-'::text || vw.group_id::text AS id,
    vw.role,
    vw.type,
    vw.match
 FROM mercury_group_rules_vw vw;

CREATE OR REPLACE VIEW mercury_notify_vw
 AS
 WITH
 tt as (
    SELECT DISTINCT 
        vw.name,
        unnest(vw."values") AS rules
    FROM mercury_registry_vw vw
    WHERE vw.space::text = 'mercury.notify'::text
 )
 SELECT 
    tt.name,
    split_part(tt.rules::text, ' '::text, 1) AS match,
    split_part(tt.rules::text, ' '::text, 2) AS event,
    split_part(tt.rules::text, ' '::text, 3) AS method,
    split_part(tt.rules::text, ' '::text, 4) AS url
 FROM tt;
