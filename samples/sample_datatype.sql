create table inttest (
    k_sml smallint,
    k_big bigint,
    k_int integer
);

create table serialtest (
    k_ser serial,
    k_sml smallserial,
    k_big bigserial
);

create table testreal (
    k_dbl double precision,
    k_num numeric,
    k_dec decimal,
    k_rea real
);

create table testtext (
    k_txt text,
    k_ch1 char,
    k_ch2 character,
    k_vc1 character varying (3),
    k_vc2 varchar (4)
);

create table testquote (
    K_Quo integer,
    kquot varchar (3),
    "KQuo" int
);
