{% macro udf() %}

create schema if not exists analysis;
use schema analysis;

create or replace function hash_key(input array) returns text called on null input as
$$
select sha2(array_to_string(array_compact(input), ''))
$$;

create or replace function local_time(ts timestamp_tz) returns timestamp_tz strict immutable as
$$
select convert_timezone('America/Los_Angeles', ts)
$$;

create or replace function local_time(ts timestamp_ntz) returns timestamp_tz strict immutable as
$$
select convert_timezone('America/Los_Angeles', ts)
$$;

create or replace function phone_number(input text) returns text language javascript as
$$
if (INPUT === null || INPUT === undefined) {
    return null;
}

let input = INPUT.replace(/[^0-9]+/g, '').replace(/^[01]{1}/g, '');
if (input !== '' && input.length === 10 && parseInt(input.substring(0, 3)) >= 201) {
    return '+1' + input;
}

return null;
$$;

create or replace function rawurldecode(input text) returns text language javascript as
$$
if (INPUT === null || INPUT === undefined) {
    return null;
}

return decodeURIComponent(INPUT);
$$;

create or replace function email_address(input text) returns text as
$$
lower(iff(trim(input) rlike '^[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$', trim(input), null))
$$;

{% endmacro %}