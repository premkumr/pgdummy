# pgdummy
- `pgdummy` generates fake/dummy data for a given postgresql schema file
- For various pre-defined data types, it preset generators.
- Custom generators can be specified via a yaml config file
- Almost all of [Faker's generators](https://faker.readthedocs.io/en/master/providers/baseprovider.html) can be used

# Installation
```
pip install https://github.com/premkumr/pgdummy/releases/download/v0.5.1/pgdummy-0.5.1-py3-none-any.whl
```

# Usage
- For a given schema file, first generate the config file
- Modify the config file to setup customized generators
---
### Sample Schema
```
CREATE TABLE public.pilot (
    id int4 NOT NULL,
    name text NOT NULL,
    code bpchar(3) NOT NULL
);

CREATE TABLE public.airport (
    name text NOT NULL,
    code bpchar(3) NOT NULL
);
```
### Generate config file
```
pgdummy --schema test.schema.sql --generate-config --config test.conf.yaml

> cat test.conf.yaml
tables:
    pilot:
        id:
            generator: integer
        name:
            generator: string
        code:
            generator: string
    airport:
        name:
            generator: string
        code:
            generator: string
```

### Generate the data
- The stdout can be directly piped to `psql` as all logs go to stderr.
- By default `COPY` commands are generated. 
- `--format insert` can be specified to generate `INSERT INTO` commands
```
pgdummy --schema test.schema.sql  --config test.conf.yaml

--
-- data for [public.pilot]
--

COPY public.pilot ( id,name,code ) FROM stdin;
142	HBCRBJSU	VQI
149	TWVNVLLE	SIH
953	PDOHOFGZ	AOA
913	BCWZMMOH	ZNE
858	UAAORNZB	AEZ
\.

--
-- data for [public.airport]
--

COPY public.airport ( name,code ) FROM stdin;
CCSXEZZR	XLH
WUZUWXCQ	OPW
GSBHPKJY	XRY
HLPSMQAM	NDV
HHUKNJLW	YET
\.
```
### Generators
---
## string
- Generates a random string. takes `max`, `min` as options to specify length range
- `pattern` option can be set to generate specific type of strings
- eg. `A%-??` --> `['A1-FG', 'A4-JK', 'A0-NB' ...]`
- 
## oneof
- picks one from the items specified
- `items: ['SFO', 'MAA', 'DEL']`

## sequence
- ordered number will be generated eg. `1,2,3,4...`
- option `start` - set the starting number (default:1)
- option `step` - increment (default:1)

## foreign
- foreign key dependencies can be specified here
- First the foreign key columns are generated and from those values, the current column is filled.
- mandatory option `key` is of the format `table.column`

## uuid4
-  Generates a v4 uuid eg : `cf2f7df8-ed52-4b2e-aee4-7e4aab21c051`

## ip address (ipv4/ipv6)
- `generator: ipv4` eg. `69.129.78.81`
- `generator: ipv6` eg. `648a:664c:51d3:da:b9c0:139a:579:ef2f` 

## cidr blocks
-  same as ip addresses with option `network : True`
-  eg. `208.237.0.0/16 , 54fb:7e8e::/32`

## Other Generators from Faker
- `first_name` - eg. `Alyssa,Phillip,Melanie`
- `last_name` - eg. `Moody,Moore,Williams`
- `name` - eg. `Jennifer Fowler, Malik Holt, Douglas Walters`
- `zipcode` - eg. `94085,72280,53792`
- `city` - eg. `Juanfurt, Debrastad, Gabrielfort`
- `year` - eg. `1977,1984,2022`
- `text` - eg. `Provide debate suggest treat least doctor quality. Fear color example increase`
- `word` - eg. `process,space,building`
- `country` - eg. `Denmark`

### Special options
## distinct
- add this option to any generator to restrict the no.of unique items generated
- `distinct: 4` --> The same 4  items will be generated repeatedly

## unique
- When `unique : true` is set, then all elements generated will be unique.

### Unique constraints (Multi-Column)
## __unique
- This table level section of lists to specify uniqueness of a set of columns.
- specified as a list or comma(,) separated columns eg. 
    __unique:
        -   [col1, col2, col3]
        -   col1, col2, col3

### Other settings
## __numrows
- Table level setting to restrict the no.of rows generated for a table (overrides cmd-line)
- specifying `__numrows : 10`, generates just `10` rows for that table

