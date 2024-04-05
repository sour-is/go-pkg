CREATE TABLE IF NOT EXISTS mercury_spaces
(
    space character varying NOT NULL unique,
    id integer NOT NULL CONSTRAINT mercury_namespace_pk PRIMARY KEY autoincrement,
    notes json NOT NULL DEFAULT '[]',
    tags json NOT NULL DEFAULT '[]',
    trailer json NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS mercury_values
(
    id integer NOT NULL,
    seq integer NOT NULL,
    name character varying NOT NULL,
    "values" json NOT NULL DEFAULT '[]',
    tags json NOT NULL DEFAULT '[]',
    notes json NOT NULL DEFAULT '[]',
    CONSTRAINT mercury_values_pk PRIMARY KEY (id, seq)
);

drop view if exists mercury_registry_vw;
CREATE VIEW if not exists mercury_registry_vw
 AS
 SELECT 
    s.id,
    v.seq,
    s.space,
    v.name,
    v."values",
    v.notes,
    v.tags,
    s.trailer
 FROM mercury_spaces s
 JOIN mercury_values v ON s.id = v.id;

drop view if exists mercury_groups_vw;
CREATE VIEW if not exists mercury_groups_vw
 AS
 SELECT DISTINCT 
    j.value AS user_id,
    vw.name AS group_id
 FROM mercury_registry_vw vw, json_each(vw."values") j
 WHERE vw.space = 'mercury.groups';

drop view if exists mercury_group_rules_vw;
CREATE VIEW if not exists mercury_group_rules_vw
 AS
 WITH 
 tt as (
    SELECT DISTINCT
        vw.name AS group_id,
        j.value AS rules
    FROM mercury_registry_vw vw, json_each(vw."values") j
    WHERE vw.space = 'mercury.policy'
 )
 SELECT tt.group_id,
    tt.rules rule,
    '' AS role,
    '' AS type,
    ''AS match
 FROM tt;

drop view if exists mercury_user_rules_vw;
CREATE VIEW if not exists mercury_user_rules_vw
 AS
 WITH
 tt as (
    SELECT DISTINCT 
        vw.name AS group_id,
        j.value AS rules
    FROM mercury_registry_vw vw, json_each(vw."values") j
    WHERE vw.space = 'mercury.policy'
 )
 SELECT 
    g.user_id,
    tt.rules rule,
    '' AS role,
    '' AS type,
    '' AS match
 FROM mercury_groups_vw g
 JOIN tt ON g.group_id = tt.group_id;

drop view if exists mercury_rules_vw;
CREATE VIEW if not exists mercury_rules_vw
 AS
 SELECT 
    'U-' || vw.user_id AS id,
    vw.rule,
    vw.role,
    vw.type,
    vw.match
 FROM mercury_user_rules_vw vw
 UNION
 SELECT 
    'G-' || vw.group_id AS id,
    vw.rule,
    vw.role,
    vw.type,
    vw.match
 FROM mercury_group_rules_vw vw;

drop view if exists mercury_notify_vw;
CREATE VIEW if not exists mercury_notify_vw
 AS
 WITH
 tt as (
    SELECT DISTINCT 
        vw.name,
        j.value AS rules
    FROM mercury_registry_vw vw, json_each(vw."values") j
    WHERE vw.space = 'mercury.notify'
 )
 SELECT 
    tt.name,
    tt.rules rule,
    substr(tt.rules, 1, instr(tt.rules, ' ')-1) AS match,
    substr(tt.rules, instr(tt.rules, ' ')+1, instr(substr(tt.rules, instr(tt.rules, ' ')+1), ' ')-1) AS event,
    '' AS method,
    '' as url
 FROM tt;
