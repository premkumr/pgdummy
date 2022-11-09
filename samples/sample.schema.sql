CREATE TABLE public.pilot (
    id int4 NOT NULL,
    name text NOT NULL,
    code bpchar(3) NOT NULL
);

CREATE TABLE public.airport (
    name text NOT NULL,
    code bpchar(3) NOT NULL
);

