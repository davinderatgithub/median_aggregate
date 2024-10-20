CREATE OR REPLACE FUNCTION _median_transfn(state internal, val anyelement)
RETURNS internal
AS 'MODULE_PATHNAME', 'median_transfn'
PARALLEL SAFE
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _median_mtransfn(state internal, val anyelement)
RETURNS internal
AS 'MODULE_PATHNAME', 'median_mtransfn'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _median_finalfn(state internal, val anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'median_finalfn'
PARALLEL SAFE
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _median_combinefunc(state1 internal, state2 internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'combine_median_state'
PARALLEL SAFE
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _median_serialfunc(state internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'serialize_median_state'
PARALLEL SAFE
LANGUAGE C IMMUTABLE;
CREATE OR REPLACE FUNCTION _median_deserialfunc(serial_data bytea, state internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'deserialize_median_state'
PARALLEL SAFE
LANGUAGE C IMMUTABLE;

DROP AGGREGATE IF EXISTS median (ANYELEMENT);
CREATE AGGREGATE median (ANYELEMENT)
(
    sfunc = _median_transfn,
    stype = internal,
    finalfunc = _median_finalfn,
    finalfunc_extra,
    MSFUNC = _median_transfn,
    MINVFUNC = _median_mtransfn,
    MSTYPE = internal,
    MFINALFUNC = _median_finalfn,
    MFINALFUNC_EXTRA,
    COMBINEFUNC = _median_combinefunc,
    SERIALFUNC = _median_serialfunc,
    DESERIALFUNC = _median_deserialfunc,
    PARALLEL = SAFE
);
